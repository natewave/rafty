(ns kvstore.router
  (:require [reitit.ring :as ring]
            [reitit.swagger :as swagger]
            [reitit.swagger-ui :as swagger-ui]
            [muuntaja.core :as m]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.coercion.malli :as coercion-malli]
            [reitit.ring.coercion :as coercion]
            [reitit.dev.pretty :as pretty]
            [reitit.spec :as rs]

            [kvstore.store :as store]
            [kvstore.rpc :as rpc]))

(def swagger-docs
  ["/swagger.json"
   {:get {:no-doc true
          :swagger {:basePath "/"
                    :info {:title "KVStore"
                           :description "KVStore Rest API"
                           :version "1.0.0"}}
          :handler (swagger/create-swagger-handler)}}])
(def router-config
  {;:reitit.middleware/transform dev/print-request-diffs : trace a request
   :validate rs/validate                                    ;; validate reitit params (handlers, etc)
   :exception pretty/exception
   :data {:coercion coercion-malli/coercion
          :muuntaja m/instance                              ;; content negotiation
          :middleware [swagger/swagger-feature
                       muuntaja/format-middleware
                       coercion/coerce-request-middleware
                       coercion/coerce-response-middleware]}})

(defn routes
  [env]
  (ring/ring-handler
    (ring/router
      [swagger-docs
       ["/v1"
        (store/routes env)
        (rpc/routes env)]]
      router-config)
    (ring/routes
      (swagger-ui/create-swagger-ui-handler {:path "/"}))))