(require '[clojure.edn :as edn])
(defn write-object
  "Serializes an object to disk so it can be opened again later.
   Careful: It will overwrite an existing file at file-path."
  [obj file-path]
    (with-open [wr (io/writer file-path)]
      (.write wr (pr-str obj))))

(store-atom-safely WORLD "world_data_2015-01-21.edn")
(store-atom-safely LAST_VISITS "last_visits_data_2015-01-21.edn")
(store-atom-safely SELF_LOOP_PCT "self_loop_pcts_data_2015-01-21.edn")
(store-atom-safely SUBREDDIT_COUNTS "subreddit_counts_data_2015-01-21.edn")
(store-atom-safely SUBREDDIT_USER_COUNTS "subreddit_user_counts_data_2015-01-21.edn")
(store-atom-safely HISTORIES "histories_data_2015-01-21.edn")
(def TRANSITS (ref 1336145613))
(def SIMULATION_ID (atom 1782514))
(def slurm slurp)
(def world (read-string (slurm "world_data_2015-01-21.edn")))
(def last_visits (edn/read-string (slurm "last_visits_data_2015-01-21.edn")))
(def self_loop_pct (edn/read-string (slurm "self_loop_pcts_data_2015-01-21.edn")))
(def subreddit_counts (edn/read-string (slurm "subreddit_counts_data_2015-01-21.edn")))
(def subreddit_user_counts (edn/read-string (slurm "subreddit_user_counts_data_2015-01-21.edn")))
(def histories (edn/read-string (slurm "histories_data_2015-01-21.edn")))


(def buggy-key (keyword ""))

(def replacement-key :___THIS_IS_FUCKED___)

(defn store-atom-safely
  [atom file]
  (let [structure @atom
        safe-structure (clojure.walk/postwalk-replace
                        {buggy-key replacement-key}
                        structure)]
    (spit file (pr-str safe-structure))))

    (reset! WORLD (map))