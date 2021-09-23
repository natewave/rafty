(ns user
  (:require [integrant.repl :as ig-repl]
            [integrant.core :as ig]
            [integrant.repl.state :as state]
            [kvstore.server]
            [clj-rocksdb :as rocksdb]))

(ig-repl/set-prep!
  (fn [] (-> "resources/config.edn" slurp ig/read-string)))

(def go ig-repl/go)
(def halt ig-repl/halt)
(def reset ig-repl/reset)
(def reset-all ig-repl/reset-all)

(def app (-> state/system :kvstore/app))
(def db  (-> state/system :db/rocksdb))


(comment

  (:env app)

  (rocksdb/put db "hey" "hello")
  (rocksdb/get db "hey")

  (-> (app {:request-method :get
            :uri "/v1/store/hey"})
      :body
      (slurp))

  (-> (app {:request-method :post
            :uri "/v1/store"
            :body-params {:store/key "hey" :store/value "worked"}})
      :body
      (slurp))

  (-> (app {:request-method :put
            :uri "/v1/store/hey"
            :body-params {:value "new-value"}}))

  (-> (app {:request-method :delete
            :uri "/v1/store/hey"}))

  (require '[clojure.pprint :refer [pprint]])
  (require '[reitit.core])
  (require '[reitit.coercion])
  (require '[reitit.coercion.spec])

  (defn rand-str [len]
    (apply str (take len (repeatedly #(char (+ (rand 26) (if (> 0.5 (rand 1))
                                                           65
                                                           97)))))))
  (rand-str 10)
  (char (+ (rand 26) 100))

  (go)
  (halt)
  (reset)
  )

