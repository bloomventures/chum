(ns humandb.transact
  (:require
    [datascript.core :as d]))

(defn write!
  "Given location and doc, updates file with yaml doc"
  [location doc]
  ; TODO
  )

(defn get-doc
  "Given eid, returns doc containing all attributes (and embedded docs)"
  [conn eid]
  ; TODO, also get nested docs
  ; will need to look up schema AND src to see if should embedded
  (d/pull conn '[*] eid))

(defn remove-metadata
  "Recursively removes metadata (keywords in :db/* namespace)"
  [doc]
  (->> doc
       (reduce (fn [memo [k v]]
                 (cond
                   (= "db" (namespace k))
                   memo

                   (map? v)
                   (assoc memo k (remove-metadata v))

                   (and (vector? v) (map? (first v)))
                   (assoc memo k (map remove-metadata v))

                   :else
                   (assoc memo k v)))
               {})))

(defn get-parent [conn eid]
  ; TODO
  )

(defn save-doc!
  [conn eid]

  (if-let [parent-eid (get-parent conn eid)]
    (save-doc! conn parent-eid)

    (let [doc (get-doc conn eid)
          location (:__src__ doc)]
      (write! location (remove-metadata doc)))))

(defn affected-docs [txs]
  (set (map second txs)))

(defn transact! [conn txs]
  (d/transact conn txs)
  ; TODO could identify parents of each affected-doc
  ; to avoid saving a top-level doc multiple times
  (doseq [doc (affected-docs txs)]
    (save-doc! conn doc)))

