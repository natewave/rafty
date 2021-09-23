(ns kvstore.utils
  (:require [clojure.string :as str]))

;; transforms peers config
;; from: "id1@address1;id2@address2;..."
;; to  : {"id1" {:address "address1"}, "id2" {:address "address2"}}
(defn parse-peers [peers-string]
  (->> (str/split peers-string #";")
       (map (fn [peer]
              (let [[peer-id address] (str/split peer #"@")]
                [peer-id {:address address}])))
       (into {})))

(defn dlog [msg]
  (println msg))

(defn majority
  [n]
  (-> (/ n 2)
      (Math/floor )
      (int)
      (+ 1)))

(defn entries-from-index
  [entries i]
  (if (< i 0)
    (throw (Exception. (str "Dropping entries at illegal index " i)))
    (into [] (drop i entries))))

(defn truncate-entries
  [entries from]
  (into [] (take (inc from) entries)))

;; finds the median, biasing towards lower values if tie.
(defn median
  [coll]
  (let [size (count coll)]
    (-> coll
        sort
        (nth (- size (majority size))))))
