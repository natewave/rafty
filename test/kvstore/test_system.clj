(ns kvstore.test-system
  (:require [clojure.test :refer :all]
            [ring.mock.request :as mock]
            [muuntaja.core :as m]
            [clojure.core.async :as async]
            [ring.adapter.jetty :as jetty]
            [environ.core :refer [env]]
            [kvstore.router :as router]
            [clj-rocksdb :as rocksdb]
            [taoensso.nippy]
            [tick.alpha.api :as t]
            [kvstore.utils :as ut]
            [clj-http.client :as http]))

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn create-node [port raft-config]
  (with-out-str
    (with-redefs [kvstore.raft/node {:id                       (atom (:id raft-config))
                                     :state                    (ref :follower)
                                     :storage-ref              (atom nil)
                                     :election-deadline        (atom (t/now))
                                     :step-down-deadline       (atom (t/now))
                                     :last-replication         (atom (t/now))
                                     :min-replication-interval 50
                                     :heartbeat-interval       500
                                     :leader-id                (atom nil)
                                     :term                     (ref 0)
                                     :peers                    (atom (ut/parse-peers (:peers raft-config)))
                                     :voted-for                (atom nil)
                                     :log-entries              (atom [{:term 0 :op nil}])
                                     :commit-index             (atom 0)
                                     :next-index               (atom nil)
                                     :match-index              (atom nil)
                                     :last-applied             (atom 0)}]
      (let [db-dir (str "/tmp/" (rand-str 6))
            db (rocksdb/create-db db-dir
                                  {:key-encoder taoensso.nippy/freeze :key-decoder taoensso.nippy/thaw
                                   :val-encoder taoensso.nippy/freeze :val-decoder taoensso.nippy/thaw})
            app (router/routes {:db db})
            jetty (jetty/run-jetty app {:port port :join? false})]
        (reset! (:storage-ref kvstore.raft/node) db)
        (kvstore.raft/init raft-config)
        {:app app
         :db-dir db-dir
         :db db
         :jetty jetty}))))

(defn destroy-node
  [db db-dir jetty]
  (.close db)
  (rocksdb/destroy-db db-dir)
  (.stop jetty))

(def nodes (atom nil))
(defn create-nodes
  []
  {:node1 (create-node 3001 {:id "1" :peers "2@http://localhost:3002;3@http://localhost:3003"})
   :node2 (create-node 3002 {:id "2" :peers "1@http://localhost:3001;3@http://localhost:3003"})
   :node3 (create-node 3003 {:id "3" :peers "1@http://localhost:3001;2@http://localhost:3002"})})

(defn destroy-nodes
  []
  (doall (map (fn [{:keys [db-dir db jetty]}]
                (destroy-node db db-dir jetty)) (vals @nodes))))

(defn node-fixtures
  [f]
  (reset! nodes (create-nodes))
  (f)
  (destroy-nodes)
  (reset! nodes nil))

(def node1 "http://localhost:3001")
(def node2 "http://localhost:3002")
(def node3 "http://localhost:3003")

(defn endpoint
  ([node-id method uri]
   (endpoint node-id method uri nil))
  ([node-id method uri params]
   (case method
     :get (http/get (str node-id uri) {:as :json})
     :post (http/post (str node-id uri) {:form-params params
                                         :content-type :json
                                         :as :json}))))

(comment
  (def a (create-nodes))

  (let [reply (http/get "http://localhost:3001/v1/rpc/system-info" {:as :json})]
    (println (:body reply)))

  (doall (map (fn [{:keys [db-dir db jetty]}]
                (destroy-node db db-dir jetty)) (vals a)))

  (destroy-nodes)
  (let [request (test-endpoint :get "/v1/store/hey" :node1)
        decoded-request (m/decode-response-body request)]
    (assoc request :body decoded-request))

  (test-endpoint :post "/v1/urls" {:original-url "http://google.com"}))
