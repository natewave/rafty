(ns kvstore.raft
  (:require [tick.alpha.api :as t]
            [clojure.core.async :as async]
            [kvstore.utils :as ut]
            [clj-http.client :as http]
            [clj-rocksdb :as rocksdb]))

(def node {:id                       (atom nil)
           :state                    (ref :follower)
           :storage-ref              (atom nil)
           :election-deadline        (atom (t/now))                     ;; next election
           :step-down-deadline       (atom (t/now))                     ;; when to step down automatically
           :last-replication         (atom (t/now))                     ;; when did we last replicate ?
           :min-replication-interval 50                                 ;; to avoid replicating too frequently
           :heartbeat-interval       500                                ;; todo try with 50?
           :leader-id                (atom nil)                         ;; to internally proxy client requests to leader
           :term                     (ref 0)
           :peers                    (atom {})
           :voted-for                (atom nil)
           :log-entries              (atom [{:term 0 :op nil}])
           :commit-index             (atom 0)                           ;; the highest committed entry in the log
           :next-index               (atom nil)                         ;; map of peer-id to the next index to replicate
           :match-index              (atom nil)                         ;; map of peer-id to the highest log entry known to be replicated
           :last-applied             (atom 0)})                         ;; last applied entry to the state machine


(defn append-entries [entries]
  (doall (map (fn [entry] (swap! (:log-entries node) conj entry)) entries)))

(defn send-message
  [peer-id msg]
  (let [peer-address (get-in @(:peers node) [peer-id :address])]
    (http/post (str peer-address "/v1/rpc") {:form-params {:rpc/message msg}
                                             :content-type :json
                                             :as :json})))

(defn broadcast-message
  [msg]
  (doall (map (fn [peer-id]
                (async/go (send-message peer-id msg)))) (keys @(:peers node))))

(defn advance-term!
  [new-term]
  ;; terms are monotonic, they should only increase
  (dosync
    (if (> new-term @(:term node))
      (do
        (ref-set (:term node) new-term)
        (reset! (:voted-for node) nil))
      (throw (Exception. "[!] Terms can't go backwords!")))))

(defn apply-op!
  [op]
  (let [action (-> op :type (keyword))]
    (case action
      :put (try
             (rocksdb/put @(:storage-ref node) (:key op) (:value op))
             (catch Exception e
               (clojure.pprint/pprint (str "[-] Error accessing db: " (.getMessage e)))))
      :delete (try
                (rocksdb/delete @(:storage-ref node) (:key op))
                (catch Exception e
                  (clojure.pprint/pprint (str "[-] Error accessing db: " (.getMessage e)))))
      nil)))

(defn advance-state-machine!
  []
  (dosync
    (let [{:keys [last-applied commit-index log-entries]} node]
      (while (< @last-applied @commit-index)
        (swap! last-applied inc)
        (let [req (-> (nth @log-entries @last-applied) :op)]
          (ut/dlog (str "[+] Applying op to state machine " req))
          (apply-op! req))))))

(defn advance-commit-index!
  []
  (dosync
    (let [{:keys [state match-index commit-index term log-entries]} node]
      (when (= @state :leader)
        ;; any entries that are committed on a majority of peers are considered committed
        ;; that's the median of the matching indices
        (let [n (ut/median (vals @match-index))]
          (when (and (< @commit-index n)
                     (= @term (-> (nth @log-entries n) :term)))
            (do
              (ut/dlog (str "[+] Committing index now " n))
              (reset! commit-index n)))))
      (advance-state-machine!))))

;; random timeout between 150 and 300 milliseconds
(defn election-timeout []
  (-> (rand-int 1500)
      (+ 1500)
      (t/new-duration :millis)))

(defn reset-election-deadline!
  []
  (let [next-deadline (t/+ (t/now) (election-timeout))]
    (reset! (:election-deadline node) next-deadline)))

(defn reset-step-down-deadline!
  []
  (let [next-deadline (t/+ (t/now) (election-timeout))]
    (reset! (:step-down-deadline node) next-deadline)))

(defn become-follower!
  []
  (dosync
    (let [{:keys [state match-index next-index leader-id term]} node]
      (ref-set state :follower)
      (reset! match-index nil)
      (reset! next-index nil)
      (reset! leader-id nil)
      (reset-election-deadline!)
      (ut/dlog (str "Became follower for term " @term)))))

(defn maybe-step-down!
  [remote-term]
  (dosync
    (let [term (:term node)]
      (if (< @term remote-term)
        (do
          (ut/dlog (str "[!] Stepping down: remote term " remote-term " higher than local term " @term))
          (advance-term! remote-term)
          (become-follower!))))))

(defn become-leader!
  []
  (let [{:keys [state log-entries state leader-id last-replication match-index next-index term peers]} node]
    (if (not= @state :candidate)                               ;; can only transition to leader from candidate
      (throw (Exception. "Should be a candidate!"))
      (dosync
        (let [next-log-size (count @log-entries)]
          (ref-set state :leader)
          (reset! leader-id nil)
          (reset! last-replication (t/epoch))                   ;; to start sending heartbeats immediately
          (reset! match-index {})
          (reset! next-index {})

          (doall (map (fn [peer-id]
                        (swap! next-index assoc peer-id next-log-size)
                        (swap! match-index assoc peer-id 0)
                        ) (keys @peers)))

          (reset-step-down-deadline!)
          (ut/dlog (str "[+] Became leader for term " @term)))))))

;; If leader, replicate unacknowledged log entries to followers.
;; Also serves as a heartbeat.
(defn replicate-logs!
  [force?]
  (dosync
    (let [{:keys [state min-replication-interval next-index log-entries
                  heartbeat-interval term id commit-index match-index peers last-replication]} node
          elapsed-time (t/between @last-replication (t/now))      ;; time since we last replicated
          replicated? (atom false)                                ;; set to true if at least one follower replicated
          ]
      (if (and (= @state :leader)
               (t/< (t/new-duration min-replication-interval :millis) elapsed-time))  ;; check if leader and enough time elapsed
          (do
            ;; for every peer, we'll look at the next index we want to send to it
            ;; and take all higher entries from the log-entries
            (doall (map (fn [peer-id]
                          (let [ni (get @next-index peer-id)
                                entries (ut/entries-from-index @log-entries ni)]
                            ;; send append-entries if new entries or for heartbeat
                            (if (or (> (count entries) 0)
                                    (t/< (t/new-duration heartbeat-interval :millis) elapsed-time))
                              (do
                                (when (not (empty? entries))
                                  (ut/dlog (str "[!] Replicating indices {" ni "+} to peer #" peer-id ": " entries)))
                                (reset! replicated? true)
                                (try
                                  (let [prev-index (- ni 1)
                                        reply (send-message peer-id {:rpc/type :append-entries
                                                                     :rpc/term @term
                                                                     :rpc/leader-id @id
                                                                     :rpc/prev-log-index prev-index
                                                                     :rpc/prev-log-term (-> (nth @log-entries prev-index) :term)
                                                                     :rpc/entries entries
                                                                     :rpc/leader-commit @commit-index})
                                        reply-term (-> reply :body :rpc/term)
                                        success?   (-> reply :body :rpc/success)
                                        #_(clojure.pprint/pprint (str "received: " (:body reply) ", sent: " {:rpc/type :append-entries
                                                                                                      :rpc/term @term
                                                                                                      :rpc/leader-id @id
                                                                                                      :rpc/prev-log-index prev-index
                                                                                                      :rpc/prev-log-term (-> (nth @log-entries prev-index) :term)
                                                                                                      :rpc/entries entries
                                                                                                      :rpc/leader-commit @commit-index}))]
                                    (maybe-step-down! reply-term)
                                    (if (and (= @state :leader)
                                             (= @term reply-term))
                                      (do
                                        (reset-step-down-deadline!)
                                        (if success?
                                          ;; Excellent, these entries are now replicated!
                                          (do
                                            (swap! next-index assoc peer-id (max ni (+ ni (count entries))))
                                            (swap! match-index assoc peer-id (max (get @match-index peer-id)
                                                                                  (+ ni (count entries) -1)))
                                            (when (not (empty? entries)) ;; avoid logging when the replication is only for sending a heartbeat
                                              (ut/dlog (str "[+] Peer "peer-id" successfully replicated logs, next index " (get @next-index peer-id))))
                                            (advance-commit-index!))
                                          ;; We didn't match; back up our next index for this node
                                          (swap! next-index assoc peer-id prev-index))))
                                    )
                                  (catch Exception e (ut/dlog (.getMessage e)))))))) (keys @peers)))
            (when @replicated?
              (reset! last-replication (t/now)))
            )
        ))))

(defn request-votes!
  []
  (let [{:keys [id term log-entries peers state term]} node
        votes (atom #{@id})
        request-term @term
        message {:rpc/type :request-vote
                 :rpc/term @term
                 :rpc/candidate-id @id
                 :rpc/last-log-index (count @log-entries)
                 :rpc/last-log-term (-> @log-entries
                                        last
                                        :term)}]
    (doall (map (fn [peer-id]
                  (async/go
                    (try
                      (ut/dlog (str "[!] Requesting vote from peer " peer-id ". Message=" message))
                      (let [reply (send-message peer-id message)
                            remote-term (-> reply :body :rpc/term)
                            vote-granted? (-> reply :body :rpc/vote-granted)
                            cluster-size (count (keys @peers))]
                        (dosync
                          (reset-step-down-deadline!)
                          (maybe-step-down! remote-term)
                          (if (and (= @state :candidate)
                                   (= @term remote-term)
                                   (= @term request-term)
                                   vote-granted?)
                            (do
                              (swap! votes conj peer-id)
                              (when (<= (ut/majority cluster-size) (count @votes)) ;; we have a majority of votes for this term!
                                (become-leader!)))))
                        )
                      (catch Exception e (ut/dlog (str "[-] Error requesting vote from peer(" peer-id ")" (.getMessage e))))
                      ))) (keys @peers)))))

(defn become-candidate!
  []
  (let [{:keys [id term state voted-for leader-id]} node]
    (dosync
      (ref-set state :candidate)
      (advance-term! (inc @term))
      (reset! voted-for @id)
      (reset! leader-id nil)
      (reset-election-deadline!)
      (reset-step-down-deadline!)
      (request-votes!)
      (ut/dlog (str "[!] Became candidate for term " @term)))))

(defn run-election-timer
  []
  (async/go
    (loop []
      (let [{:keys [state election-deadline]} node]
        (when (t/< @election-deadline (t/now))                  ;; election deadline passed
          (if (= @state :leader)
            (reset-election-deadline!)
            (become-candidate!)))

        ;; wait 10 milliseconds and start again
        ;; theoretically this could sleep for the whole election timeout
        (async/<! (async/timeout 100))
        (recur)))))

(defn run-leader-step-down-timer
  []
  (async/go
    (loop []
      (let [{:keys [state step-down-deadline]} node]
        (when (and (t/< @step-down-deadline (t/now))
                   (= @state :leader))
          (ut/dlog (str "[!] Stepping down as a leader: no ACKs received for the duration of step-down-deadline"))
          (become-follower!))
        (async/<! (async/timeout 100))
        (recur)))))

(defn run-replication-timer
  []
  (async/go
    (loop []
      (replicate-logs! false)
      (async/<! (async/timeout 50))
      (recur))))

;; RPC proxy
(defmulti handle-message (fn [message] (-> message :rpc/type (keyword))))

(defmethod handle-message :request-vote [message]
  (dosync
    (let [{:keys [log-entries term voted-for]} node
          request-term (:rpc/term message)
          grant-vote?  (atom false)
          last-log-term (-> @log-entries (last) :term)
          remote-last-log-term (:rpc/last-log-term message)
          log-size (count @log-entries)
          remote-log-size (:rpc/last-log-index message)
          candidate-id (:rpc/candidate-id message)
          local-term @term]
      (maybe-step-down! request-term)
      (cond
        (< request-term local-term)
        (ut/dlog (str "[-] Candidate " candidate-id " term " request-term " lower than local term " local-term ", not granting vote."))

        (some? @voted-for)
        (ut/dlog (str "[-] Already voted for " @voted-for " for term " local-term ", not granting vote to candidate " candidate-id))

        (< remote-last-log-term last-log-term)
        (ut/dlog (str "[-] Have log entries from term " last-log-term " which is newer than candidate " candidate-id " term " remote-last-log-term ", not granting vote."))

        (and (= remote-last-log-term last-log-term)
             (< remote-log-size log-size))
        (ut/dlog (str "[-] Candidate " candidate-id " logs are both at term " last-log-term ", but our log is " log-size " and theirs is only " remote-log-size ", not granting vote."))

        :else
        (do
          (ut/dlog (str "[+] Granting vote to peer " candidate-id " for term " request-term))
          (reset! grant-vote? true)
          (reset! voted-for candidate-id)
          (reset-election-deadline!)))

      {:rpc/type :request-vote-res
       :rpc/term request-term
       :rpc/vote-granted @grant-vote?})))

(defmethod handle-message :append-entries [message]
  (dosync
    (let [{:keys [log-entries leader-id commit-index term]} node
          entry-term (:rpc/term message)
          prev-log-index (:rpc/prev-log-index message)
          prev-log-term (:rpc/prev-log-term message)
          log-entry (get @log-entries prev-log-index)
          new-entries (:rpc/entries message)
          res {:rpc/type :append-entries-res
               :rpc/term @term
               :rpc/success false}
          leader-commit (:rpc/leader-commit message)]
      (maybe-step-down! entry-term)
      (cond
        ;; reject messages from older leaders
        (< entry-term @term)
        res

        :else
        (do
          ;; Leader is ahead, remember them and reset election to not compete with them
          (reset! leader-id (:rpc/leader-id message))
          (reset-election-deadline!)

          (cond
            ;; check previous entry to see if it matches
            (< prev-log-index 0)
            (throw (Exception. (str "[!!] Received out of bounds (prev-log-index=" prev-log-index ") append-entry from " (:rpc/leader-id message))))

            ;; check disagreement on the previous term
            (or (nil? log-entry)
                (not= (:term log-entry) prev-log-term))
            res

            :else
            (let [truncated-entries (ut/truncate-entries @log-entries prev-log-index)
                  entries (into [] (concat truncated-entries new-entries))]
              (reset! log-entries entries)
              ;; advance commit pointer
              (when (< @commit-index leader-commit)
                (reset! commit-index (min (count @log-entries) leader-commit))
                (advance-state-machine!))

              (assoc res :rpc/success true))))))))

(defmethod handle-message :client-req
  [message]
  (dosync
    (let [{:keys [log-entries term state leader-id]} node]
      (if (= @state :leader)
        (do
          (swap! log-entries conj {:term @term :op (:rpc/client-message message)})
          {:received true})
        (do
          (ut/dlog (str "[!] Got client request but not a leader"))
          (if @leader-id
            (do
              (send-message @leader-id (assoc message :rpc/term @term))
              {:received true})
            (do
              (ut/dlog (str "[-] Leader unknown, can't proxy message"))
              {:error "Leader unknown, can't proxy message"})))))))

(defn init
  [env]
  (let [parsed-peers (ut/parse-peers (:peers env))]
    (reset! (:peers node) parsed-peers)
    (reset! (:id node) (:id env))
    (run-election-timer)
    (run-leader-step-down-timer)
    (run-replication-timer)))

(comment
  (become-candidate!)

  (run-election-timer)

  (advance-term! 2)

  (become-follower!)

  (append-entries [{:term 1 :op nil} {:term 2 :op nil}])
  @log-entries

  (def msg {:rpc/type :request-vote
            :rpc/term 1
            :rpc/last-log-index 0
            :rpc/last-log-term 1
            :rpc/candidate-id "me"})
  (-> (send-message "me" msg) :body)

  (handle-message msg)
  (count [1 2 3 4])
  (init)

  (t/epoch)

  )

