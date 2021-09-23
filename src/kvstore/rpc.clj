(ns kvstore.rpc
  (:require [ring.util.response :as rr]
            [kvstore.utils :as ut]
            [kvstore.raft :as raft]))


;; ---
;; handlers
;; ---
(defmulti handle-message (fn [message] (-> message :rpc/type (keyword))))

(defmethod handle-message :request-vote [message]
  (let [request-vote-res (raft/handle-message message)]
    (rr/response request-vote-res)))

(defmethod handle-message :append-entries [message]
  (let [append-entries-res (raft/handle-message message)]
    (rr/response append-entries-res)))

(defmethod handle-message :client-req [message]
  (clojure.pprint/pprint (str "handling message " message))
  (let [client-req-res (raft/handle-message message)]
    (rr/response client-req-res)))

(defmethod handle-message :default [message]
  (ut/dlog (str "Received unknown request " message))
  (rr/status 404))

(defn handle-rpc!
  []
  (fn [request]
    (let [message (-> request :parameters :body :rpc/message)]
      (handle-message message))))

(defn get-system-info
  []
  (fn [_]
    (let [{:keys [id state leader-id term commit-index]} raft/node]
      (rr/response {:id @id
                    :state @state
                    :leader-id (if (= @state :leader) @id (if @leader-id @leader-id "unknown"))
                    :current-term @term
                    :commit-index @commit-index}))))

(def rpc-message
  [:map
   [:rpc/type string?]
   [:rpc/term int?]
   [:rpc/candidate-id {:optional true} string?]
   [:rpc/last-log-index {:optional true} int?]
   [:rpc/last-log-term {:optional true} int?]
   [:rpc/vote-granted {:optional true} boolean?]
   [:rpc/leader-id {:optional true} string?]
   [:rpc/prev-log-index {:optional true} int?]
   [:rpc/prev-log-term {:optional true} int?]
   [:rpc/entries {:optional true} [:vector [:map [:term int?] [:op any?]]]]
   [:rpc/leader-commit {:optional true} int?]
   [:rpc/client-message {:optional true} any?]])

(def system-info
  [:map
   [:id string?]
   [:state keyword?]
   [:leader-id string?]
   [:current-term int?]
   [:commit-index int?]])

;; ---
;; routes
;; ---
(defn routes
  [_]
  ["/rpc" {:swagger {:tags ["Cluster RPC messages"]}}
   [""
    {:post {:handler (handle-rpc!)
            :parameters {:body [:map [:rpc/message rpc-message]]}
            :responses {200 nil}
            :summary "Handle incoming message"}}]
   ["/system-info"
    {:get {:handler (get-system-info)
           :responses {200 {:body system-info}}
           :summary "get system info"}}]])