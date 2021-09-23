(ns kvstore.store
  (:require [ring.util.response :as rr]
            [clj-rocksdb :as rocksdb]

            [kvstore.raft :as raft]))

;; ---
;; db operations
;; ---

(defn insert-kv!
  [db kv]
  (try
    (rocksdb/put db (:key kv) (:value kv))
    true
    (catch Exception _ false)))

(defn get-kv
  [db kv]
  (rocksdb/get db (:key kv)))

(defn put-kv!
  [db kv]
  (when (some? (get-kv db kv))
    (try
      (rocksdb/put db (:key kv) (:value kv))
      true
      (catch Exception _ false))))

(defn delete-kv!
  [db kv]
  (when (some? (get-kv db kv))
    (try
      (rocksdb/delete db (:key kv))
      true
      (catch Exception _ false))))

;; ---
;; handlers
;; ---
(defn create
  []
  (fn [request]
    (let [key-value (select-keys (-> request :parameters :body) [:store/key :store/value])
          op {:type :put
              :key (:store/key key-value)
              :value (:store/value key-value)}
          reply     (raft/handle-message {:rpc/type :client-req
                                          :rpc/client-message op})]
      (rr/response reply))))

(defn delete
  []
  (fn [request]
    (let [op {:type :delete
              :key (-> request :path-params :key)}
          reply     (raft/handle-message {:rpc/type :client-req
                                          :rpc/client-message op})]
      (rr/response reply))))

(defn create-kv!
  [db]
  (fn [request]
    (let [key-value (select-keys (-> request :parameters :body) [:store/key :store/value])
          inserted? (insert-kv! db {:key (:store/key key-value)
                                 :value (:store/value key-value)})]
      (if inserted?
        (rr/created (str "store/" (:store/key key-value)) {:value (:store/value key-value)})
        (rr/status 500)))))

(defn find-kv
  [db]
  (fn [request]
    (let [k (-> request :parameters :path :key)]
      (if-let [v (get-kv db {:key k})]
        (rr/response {:store/key k
                      :store/value v})
        (rr/not-found {:type :key-not-found
                       :message "key not found"
                       :data (str "key " k)})))))

(defn update-key!
  [db]
  (fn [request]
    (let [key-value (-> request :parameters :path :key)
          new-value (-> request :parameters :body :value)
          updated? (put-kv! db {:key key-value
                                :value new-value})]
      (if updated?
        (rr/status 204)
        (rr/not-found {:type :key-not-found
                       :message "key not found"
                       :data (str "key " key-value)})))))

(defn remove-kv!
  [db]
  (fn [request]
    (let [key-value (-> request :parameters :path :key)
          deleted? (delete-kv! db {:key key-value})]
      (if deleted?
        (rr/status 204)
        (rr/not-found {:type :key-not-found
                       :message "key not found"
                       :data (str "key " key-value)})))))

(def key-value
  [:map
   [:store/key string?]
   [:store/value string?]])

(def key-values
  [:vector key-value])

;; ---
;; routes
;; ---
(defn routes
  [env]
  (let [db (:db env)]
    ["/store" {:swagger {:tags ["Client facing key-value store"]}}
     [""
      {:post {:handler (create)
              :parameters {:body key-value}
              :responses {201 nil}
              :summary "Create KV"}}]
     ["/:key"
      {:get {:handler (find-kv db)
             :responses {200 {:body key-value}}
             :parameters {:path [:map [:key string?]]}
             :summary "get key value"}
       :delete {:handler (delete)
                :responses {204 nil}
                :parameters {:path [:map [:key string?]]}
                :summary "Delete KV"}}]]))
