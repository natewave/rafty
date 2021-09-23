(ns kvstore.raft-test
  (:require [clojure.test :refer :all]
            [kvstore.server :refer :all]
            [kvstore.test-system :as ts]
            [clj-http.client :as http]))


(def commit-index (atom nil))

(deftest rpc-test
  (let [key-value {:store/key (ts/rand-str 6)
                   :store/value "test"}]
    (testing "Only one leader per term"
      (let [reply1 (ts/endpoint ts/node1 :get "/v1/rpc/system-info")
            reply2 (ts/endpoint ts/node2 :get "/v1/rpc/system-info")
            reply3 (ts/endpoint ts/node3 :get "/v1/rpc/system-info")]
        (reset! commit-index (:commit-index reply1))
        (is (= (:leader-id reply1) (:leader-id reply2) (:leader-id reply3)))
        (is (= (:current-term reply1) (:current-term reply2) (:current-term reply3)))))

    (testing "Client requests are handled"
      (let [reply (ts/endpoint ts/node1 :post "/v1/store" key-value)]
        (is (= (:status reply) 200))))

    (testing "Logs are replicated"
      (Thread/sleep 2000)
      (let [reply1 (ts/endpoint ts/node1 :get (str "/v1/store/" (:store/key key-value)))
            reply2 (ts/endpoint ts/node2 :get (str "/v1/store/" (:store/key key-value)))
            reply3 (ts/endpoint ts/node3 :get (str "/v1/store/" (:store/key key-value)))]
        (is (= (:value reply1) (:value reply2) (:value reply3)))))

    (testing "Commit index is incremented after replication"
      (let [reply1 (ts/endpoint ts/node1 :get "/v1/rpc/system-info")
            reply2 (ts/endpoint ts/node2 :get "/v1/rpc/system-info")
            reply3 (ts/endpoint ts/node3 :get "/v1/rpc/system-info")]
        (is (= @commit-index (:commit-index reply1) (:commit-index reply2) (:commit-index reply3)))
        (is (= (:leader-id reply1) (:leader-id reply2) (:leader-id reply3)))
        (is (= (:current-term reply1) (:current-term reply2) (:current-term reply3)))))))

;; TODO : write tests
;;
