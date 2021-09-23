(defproject kvstore "0.1.0-SNAPSHOT"
  :description "kvstore URL shortener"
  :url "https://clojurescript.dev/kvstore"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/core.async "1.3.618"]
                 [ring "1.8.1"]
                 [integrant "0.8.0"]
                 [environ "1.2.0"]
                 [metosin/reitit "0.5.5"]
                 [clj-http "3.10.0"]
                 [cheshire "5.10.0"]
                 [kotyo/clj-rocksdb "0.1.6"]
                 [com.taoensso/nippy "3.1.1"]
                 [tick/tick "0.4.24-alpha"]
                 [metosin/malli "0.6.1"]]
  :profiles {:uberjar {:aot :all}
             :dev {:source-paths ["dev/src"]
                   :resource-paths ["dev/resources"]
                   :dependencies [[ring/ring-mock "0.4.0"]
                                  [integrant/repl "0.3.1"]]}}
  :uberjar-name "kvstore.jar")
