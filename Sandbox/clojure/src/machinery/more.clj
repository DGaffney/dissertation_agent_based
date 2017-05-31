(defn slurp-edges-map
  [day]
  (json/read-str (slurp (str (clojure.string/join [@FILEPATH "/cumulative_daily_nets_new3/"]) day)) :key-fn keyword :value-fn keywordize-edges))

(def generate-worlds
  (map (fn[day] [day (slurp-edges-map day)]) @DAYS))

