(ns kvstore.server
  (:require [ring.adapter.jetty :as jetty]
            [integrant.core :as ig]
            [environ.core :refer [env]]
            [kvstore.router :as router]
            [clj-rocksdb :as rocksdb]
            [kvstore.raft :as raft]
            [taoensso.nippy]))

(defn app
  ;; env is the config we get from ig/init-key kvstore/app
  ;; it has db config and other stuff
  [env]
  (reset! (:storage-ref raft/node) (:db env))
  (router/routes env))

(defmethod ig/prep-key :server/jetty
  [_ config]
  (merge config {:port (Integer/parseInt (env :kvstore-port))}))

(defmethod ig/prep-key :db/rocksdb
  [_ config]
  (merge config {:storage-dir (env :kvstore-storage-dir)}))

(defmethod ig/prep-key :kvstore/raft
  [_ config]
  (merge config {:id (env :kvstore-id)
                 :peers (env :kvstore-peers)}))

(defmethod ig/init-key :server/jetty
  [_ {:keys [handler port]}]
  (println (str "\n Server running on port " port))
  (jetty/run-jetty handler {:port port :join? false}))

(defmethod ig/init-key :kvstore/app
  [_ config]
  (println "\nStarted app")
  (app config))

(defmethod ig/init-key :db/rocksdb
  [_ {:keys [storage-dir]}]
  (println "\nConfigured db")
  (rocksdb/create-db storage-dir
             {:key-encoder taoensso.nippy/freeze :key-decoder taoensso.nippy/thaw
              :val-encoder taoensso.nippy/freeze :val-decoder taoensso.nippy/thaw}))

(defmethod ig/init-key :kvstore/raft
  [_ config]
  (println "\nStarted Raft")
  (kvstore.raft/init config))

(defmethod ig/halt-key! :server/jetty
  [_ jetty]
  (.stop jetty))

(defmethod ig/halt-key! :db/rocksdb
  [_ db]
  (.close db))

(defn -main
  [config-file]
  ;; ig/read-string is what understands #ig/ref laterals in the config
  (let [config (-> config-file slurp ig/read-string)]
    (-> config ig/prep ig/init)))
