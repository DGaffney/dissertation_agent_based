(ns machinery.core
  (:require [clojure.data.json :as json]
            [clojure.data.generators :as generators]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]]
            [taoensso.timbre :as timbre
                  :refer [log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env]]
            [taoensso.timbre.appenders.core :as appenders])
  (:gen-class))

(timbre/refer-timbre)
;; TODO ------------------------------------------------------------------------
; Track how many times each node is visited for each day

;; Configuration derpage

(def cli-options
  ;; An option with a required argument
  [["-w" "--walk WALK-TYPE" "Type of Walk to run"
    :default "weighted-random-walk-restart-10pc"]
    ["-p" "--path PATH" "File path for observed data" :default "../larger_data"]])

(def BATCH_SIZE 50) ; defines the batch size for walking
(def CORE_COUNT 12) ; number of CPU cores

;; STATE -----------------------------------------------------------------------
(def FUCKUPS (atom 0))
(def WORLD (atom {}))
(def NODES (atom []))
(def SELF_LOOP_PCT (ref {}))
(def SUBREDDIT_COUNTS (ref {}))
(def SUBREDDIT_USER_COUNTS (ref {}))
(def s-u-c (ref {}))
(def s-l-s-u-c (ref {}))
(def n-s-l-s-u-c (ref {}))
(def DAYS (atom []))
(def RANDOM_WALK_ALGORITHM (atom "weighted-random-walk-restart-10pc"))
(def FILEPATH (atom "../larger_data"))
(def LAST_VISITS (atom {}))
(def LAST_VISIT_SELF_LOOP (atom {}))
(def TRANSITS (ref 0))
(def ELAPSED_MS (atom 0))
(def SIMULATION_ID (int (* (rand) 10000000)))
(def FILENAME (atom @RANDOM_WALK_ALGORITHM))
;(def HISTORIES (ref []))

;; TIMING MACRO ----------------------------------------------------------------

(defmacro bench
  "Times the execution of forms, discarding their output and returning
a long in ms."
  ([& forms]
    `(let [start# (System/currentTimeMillis)]
       ~@forms
       (- (System/currentTimeMillis) start#))))


;; FILE READERS ----------------------------------------------------------------

(defn slurp-json
  "Returns a hashmap from a JSON file on disk."
  [path]
  (json/read-json (slurp path)))

(defn slurp-csv
  "Returns a list of lists from a CSV file on disk."
  [path]
  (csv/read-csv (slurp path)))

(defn slurp-csv-kv
  "Returns a hash of CSV [[a b][x y]] as {:a b, :x y}"
  [path]
  (clojure.walk/keywordize-keys
    (apply hash-map
      (flatten (slurp-csv path)))))


;; WORLD CRUD ------------------------------------------------------------------

(defn ensure-node
  "Ensures a node with a default value exists in a hashmap if it doesn't already"
  [m kw]
  (if (contains? m kw)
    m
    (assoc m kw [])))

(defn add-edge
  "Ensures the origin and destination nodes exists, and adds an edge"
  [world edge-pair]
  (let [[origin destination] edge-pair
        original-edges       (get world (keyword origin))
        new-edges            (into [] (set (conj original-edges (keyword destination))))] ; set ensures all values are unique
        (assoc world (keyword origin) new-edges)))

(defn slurp-edges
  "Loads raw edges from disk"
  [day]
  (slurp-csv (str (clojure.string/join [@FILEPATH "/edge_creation/"]) day)))

(defn keywordize-edges [key edges]
    (mapv keyword edges))

(defn slurp-edges-map
  [day]
  (json/read-str (slurp (str (clojure.string/join [@FILEPATH "/cumulative_daily_nets_new3/"]) day)) :key-fn keyword :value-fn keywordize-edges))

(defn new-nodes
  [edges]
  (into [] (clojure.set/difference (set (map (fn[e] (keyword (first e))) edges)) (set (keys @WORLD)))))

(defn read-world
  [day]
  (json/read-str (slurp (str (clojure.string/join [@FILEPATH "/worlds/"]) day))))

(defn update-world!
  [day]
  (swap! WORLD #(merge-with into % (slurp-edges-map day)))
  (if (not (= nil (keys @WORLD)))
  (reset! NODES (into [] (keys @WORLD))))
  )

(defn log-day
  [day]
;  (info (clojure.string/join [(clojure.string/join ["==================" day "=================="]) "\n" @HISTORIES "\n"]))
;  (def HISTORIES (ref []))
  )

;; STATS CRUD ------------------------------------------------------------------

(defn rand-self-loop-pct
  []
  (rand))

(defn ensure-self-loop-pct!
  "Ensures a record exists in SELF_LOOP_PCT"
  [key]
  (dosync (alter SELF_LOOP_PCT assoc key (rand-self-loop-pct))))

(defn update-self-loop-pct!
  "Updates stats to inform walkers"
  [day]
  (let [self-loops (slurp-csv-kv (str (clojure.string/join [@FILEPATH "/accumulated_self_loops/"]) day))]
    (doseq [[subreddit value] self-loops]
      (dosync (alter SELF_LOOP_PCT assoc subreddit (read-string value))))))

(defn initial-self-loop-pct
  "Creates the initial stats data"
  [world]
  (apply hash-map
    (flatten
      (map (fn [k] [k (rand-self-loop-pct)])
           (keys world)))))


;; DAYS CRUD -------------------------------------------------------------------

(defn initial-days
  "Lists the days available for the simulation"
  []
  (map #(.getName %)
    (rest
      (file-seq (clojure.java.io/file (clojure.string/join [@FILEPATH "/edge_creation/"]))))))


;; LAST VISITS CRUD ------------------------------------------------------------

(defn set-last-visit
  "Sets the last known visit for a given user"
  [username subreddit self-loop]
  (swap! LAST_VISITS assoc (keyword username) (keyword subreddit))
  (swap! LAST_VISIT_SELF_LOOP assoc (keyword username) self-loop))

(defn get-last-visit
  "Gets the last known visit for a given user"
  [username]
  (get @LAST_VISITS username))

(defn slurp-user-starts
  "Creates {username subreddit} from file"
  [day]
  (into {}
    (for [[username subreddit] (slurp-csv (str (clojure.string/join [@FILEPATH "/user_starts/"]) day))]
      [(keyword username) (keyword subreddit)])))

(defn update-last-visits!
  "Uses the user starting data to seed LAST_VISITS"
  [day]
  (let [user-starts (slurp-user-starts day)]
  (swap! LAST_VISITS merge user-starts)
  (swap! LAST_VISIT_SELF_LOOP merge (apply hash-map (flatten (map (fn[user] [user false])(keys user-starts)))))))


;; WALKERS CRUD ----------------------------------------------------------------
(defn cast-user-count-row [row]
  (let [
    username (first row)
    post-count (last row)]
  [(keyword username) (read-string post-count)]))

(defn slurp-user-counts
  "Creates [[username count] ...] from file"
  [day]
  (let [csv-data (slurp-csv (str (clojure.string/join [@FILEPATH "/user_counts/"]) day))
        parsed-csv-data (mapv cast-user-count-row csv-data)
        walk-count (reduce + (map last parsed-csv-data))]
  [parsed-csv-data walk-count]))

(defn create-walkers
  "Returns a list of walkers for a given day."
  [day]
  (slurp-user-counts day))


;; WALKING LOGIC ---------------------------------------------------------------

(defn stay-on-current-node?
  "Determines if the walker should stay on the current node"
  [current-node]
  ; first of all, if the data's broke, fix it!
  (if-not (contains? @SELF_LOOP_PCT current-node)
    (ensure-self-loop-pct! current-node))
  (> (get @SELF_LOOP_PCT current-node) (rand)))

(defn walk
  "Performs a single traverse"
  [current-node]
  (if (or (stay-on-current-node? current-node) (= 0 (count (-> @WORLD current-node))))
    [current-node false]
    (let [step (-> @WORLD current-node rand-nth)]
      (if (= nil step) [(rand-nth (keys @WORLD)) true] [step true]))))

(defn walk-with-restart
  "Performs a single traverse"
  [current-node percent]
  (if (> percent (rand))
    (if (= (count @WORLD) 0) [current-node false] [(rand-nth @NODES) true])
    (walk current-node)))

;ALTERNATIVE IDEA: pre-seed a vector with a certain number of sampled weighted values, then-rand-nth from that - small subreddits will never
;attract any since they rarely appear in this vector, but this may be much faster and a good enough approximation of what is desired.
(defn weighted-walk-with-restart
  "Performs a single traverse"
  [current-node percent]
  (if (> percent (rand))
    (if (= (count @WORLD) 0) [current-node false] [(generators/weighted @SUBREDDIT_COUNTS) true])
    (walk current-node)))

(defn random-walk
  "Performs the random walk"
  [username total-steps]
  (let [first-step  (get @LAST_VISITS username)
        first-step-self-loop  (get @LAST_VISIT_SELF_LOOP username)]
    (loop [history []]
      (if (= (count history) total-steps) ; we have as much history as steps
        history
        (if (empty? history)
          (recur (conj history [first-step first-step-self-loop]))
          (recur (conj history (walk (first (last history))))))))))

(defn random-walk-restart
  "Performs the random walk"
  [username total-steps percent]
  (let [first-step  (get @LAST_VISITS username)
        first-step-self-loop  (get @LAST_VISIT_SELF_LOOP username)]
    (loop [history []]
      (if (= (count history) total-steps) ; we have as much history as steps
        history
        (if (empty? history)
          (recur (conj history [first-step first-step-self-loop]))
          (recur (conj history (walk-with-restart (first (last history)) percent))))))))

(defn weighted-random-walk-restart
  "Performs the random walk"
  [username total-steps percent]
  (let [first-step  (get @LAST_VISITS username)
        first-step-self-loop  (get @LAST_VISIT_SELF_LOOP username)]
    (loop [history []]
      (if (= (count history) total-steps) ; we have as much history as steps
        history
        (if (empty? history)
          (recur (conj history [first-step first-step-self-loop]))
          (recur (conj history (weighted-walk-with-restart (first (last history)) percent))))))))

(defn run-random-walk
  [username total-steps]
  (cond 
    (= @RANDOM_WALK_ALGORITHM "random-walk") (random-walk username total-steps)
    (= @RANDOM_WALK_ALGORITHM "random-walk-restart-10pc") (random-walk-restart username total-steps 0.10)
    (= @RANDOM_WALK_ALGORITHM "random-walk-restart-40pc") (random-walk-restart username total-steps 0.40)
    (= @RANDOM_WALK_ALGORITHM "random-walk-restart-70pc") (random-walk-restart username total-steps 0.70)
    (= @RANDOM_WALK_ALGORITHM "random-walk-restart-90pc") (random-walk-restart username total-steps 0.90)
    (= @RANDOM_WALK_ALGORITHM "weighted-random-walk-restart-10pc") (weighted-random-walk-restart username total-steps 0.10)
    (= @RANDOM_WALK_ALGORITHM "weighted-random-walk-restart-40pc") (weighted-random-walk-restart username total-steps 0.40)
    (= @RANDOM_WALK_ALGORITHM "weighted-random-walk-restart-70pc") (weighted-random-walk-restart username total-steps 0.70)
    (= @RANDOM_WALK_ALGORITHM "weighted-random-walk-restart-90pc") (weighted-random-walk-restart username total-steps 0.90)
))

(defn run-and-measure-walk
  [walker-pair]
  (let [[username total-steps] walker-pair
        history (run-random-walk username total-steps)]
    (set-last-visit (keyword username) (first (last history)) (last (last history)))
  [username history]))

(defn update-subreddit-counts
  [subreddit-counts]
  (doall (map (fn [subreddit-count] 
    (dosync (alter SUBREDDIT_COUNTS update-in [(first subreddit-count)] (fnil (partial + (last subreddit-count)) 0)))) subreddit-counts)))
  
(defn update-subreddit-user-counts
  [subreddit-user-counts self-loop-subreddit-user-counts non-self-loop-subreddit-user-counts]
  (let [
;    updated-counts (merge-with (fn [a b] (merge-with + a b)) @s-u-c subreddit-user-counts)
    self-loop-updated-counts (merge-with (fn [a b] (merge-with + a b)) @s-l-s-u-c self-loop-subreddit-user-counts)
    non-self-loop-updated-counts (merge-with (fn [a b] (merge-with + a b)) @n-s-l-s-u-c non-self-loop-subreddit-user-counts)
    ]
;  (dosync (ref-set s-u-c updated-counts))
  (dosync (ref-set s-l-s-u-c self-loop-updated-counts))
  (dosync (ref-set n-s-l-s-u-c non-self-loop-updated-counts))))

(defn update-stats
  [walker-results]
  (let [transposed-results (apply map list walker-results)
        history-set (nth transposed-results 0)
        subreddit-count-set (nth transposed-results 1)
        subreddit-counts (apply merge-with + subreddit-count-set)
        ; subreddit-user-count-set (nth transposed-results 2)
        ; subreddit-user-counts (apply merge-with + subreddit-user-count-set)
        transit-set (nth transposed-results 3)
        ; self-loop-subreddit-user-count-set (nth transposed-results 4)
        ; self-loop-subreddit-user-counts (apply merge-with + self-loop-subreddit-user-count-set)
        ; non-self-loop-subreddit-user-count-set (nth transposed-results 5)
        ; non-self-loop-subreddit-user-counts (apply merge-with + non-self-loop-subreddit-user-count-set)
        ]
;    (dosync (alter HISTORIES conj history-set))
    (update-subreddit-counts subreddit-counts)
    ; (update-subreddit-user-counts subreddit-user-counts self-loop-subreddit-user-counts non-self-loop-subreddit-user-counts)
    (dosync (alter TRANSITS + (reduce + transit-set)))))

(defn create-batches
  [current-walker-data]
  (let [current-walkers (first current-walker-data)
        walk-sum        (last current-walker-data)
        target-count    (int (/ walk-sum (/ CORE_COUNT 0.25)))]
    (loop [accounts (shuffle current-walkers)
           group []
           groups []]
      (if (empty? accounts)
        (conj groups group)
        (let [acct (first accounts)
              next-accounts (rest accounts)
              [next-group next-groups]            
              (if (->> group 
                       (map last) 
                       (apply +) 
                       (<= target-count)) 
                [[acct] (conj groups group)]
                [(conj group acct) groups])]
          (recur next-accounts next-group next-groups))))))

(defn determine-batch-size 
  [walker-count]
  (let [batch-count (int (/ walker-count BATCH_SIZE))]
  (if (= batch-count 0)
    1
    batch-count)))

(defn create-batches
  [walkers]
  (let [walkers (first walkers)]
  (partition-all (determine-batch-size (count walkers)) walkers)))

(defn filter-subreddit-user-counts
  [histories select-self-loops]
  (into {} 
    (filter 
      (fn[user-pair] (> (count (last user-pair)) 0)) 
      (map (fn [pair] [(first pair) (frequencies 
        (map first 
          (filter 
            (fn[p] (= select-self-loops (last p))) 
            (last pair))))]) 
  histories))))
  

(defn run-batch
  [walkers]
  (let [histories (doall (map run-and-measure-walk walkers))
        sub-histories (map (fn[history] [(first history) (map first (last history))]) histories)
        subreddit-counts (frequencies (flatten (map last sub-histories)))
        subreddit-user-counts (into {} (map (fn [pair] [(first pair) (frequencies (last pair))]) sub-histories))
        self-loop-subreddit-user-counts (filter-subreddit-user-counts histories false)
        non-self-loop-subreddit-user-counts (filter-subreddit-user-counts histories true)
        transits (reduce + (vals subreddit-counts))]
    [histories subreddit-counts subreddit-user-counts transits self-loop-subreddit-user-counts non-self-loop-subreddit-user-counts]))



;; UTILITY FNs -----------------------------------------------------------------

(defn millis
  "Provides the current time in milliseconds for timing purposes"
  []
  (System/currentTimeMillis))

(defn transits-per-second
  "Provides the current number of transits per second"
  []
  (* 1000 (quot @TRANSITS @ELAPSED_MS)))

(defn total-edges
  "Counts all of the edges contained in the provided world"
  [world]
  (loop [edge-count 0
         node-keys (keys world)]
    (if (empty? node-keys)
      edge-count ; no more node keys, so we'll return the total edge count
      ; ... otherwise, continue stepping through the node keys and counting the edges for each
      (let [current-node    (first node-keys)
            current-count   (count (get world current-node))
            remaining-nodes (rest node-keys)]
        (recur (+ edge-count current-count) remaining-nodes)))))

(defn write-subreddit-user-counts [filename page slice] 
  (spit (clojure.string/join [filename page]) (json/write-str slice)))

;; YOLO ... EXCEPT IN SIMULATIONS ----------------------------------------------

(defn -main
  [& args]
  (reset! RANDOM_WALK_ALGORITHM (get (get (parse-opts args cli-options) :options) :walk))
  (reset! FILEPATH (get (get (parse-opts args cli-options) :options) :path))
  ; Set up the initial state of the universe
  (reset! DAYS (initial-days))
  (reset! FILENAME (clojure.string/join [(clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM]) ".log"]))
  (timbre/merge-config! {:appenders {:spit (merge (appenders/spit-appender {:fname @FILENAME}) {:async? true})}})
  (timbre/swap-config! assoc-in [:appenders :println :enabled?] false)
  ; BEGIN MAIN RUN LOOP
  (doseq [day (take 2000 (sort @DAYS))]
    (println @RANDOM_WALK_ALGORITHM)
    ; timestamp the start of this iteration
    (def iteration-start-ms (millis))
    ; Make world adjustments; these have to be done in sequence: world, loops, visits
    (def update-world-ms
      (bench
        (update-world! day)))

    (def update-loops-ms
      (bench
        (update-self-loop-pct! day)))

    (def update-visits-ms
      (bench
        (update-last-visits! day)))

    (def create-walkers-ms
      (bench
        (def current-walkers (create-walkers day))))

    (def run-walkers-ms
      (bench
        (def walker-results (doall ; force realization
          (pmap ; executes each of the run-batch functions in parallel
            run-batch
              (create-batches current-walkers))))))

    (def update-stats-ms
      (bench 
        (update-stats walker-results)))

    (def log-results-ms
      (bench
        (log-day day)))
    ; total time (in ms) for executing this iteration of the simulation
    (def iteration-elapsed (- (millis) iteration-start-ms))

    ; record the execution time
    (swap! ELAPSED_MS + iteration-elapsed)

    ; status strings
    (println (str "World size: " (count @WORLD)))
    (println (str "Transits: " ))
    (println (str (format "%.2f" (* (float (/ @TRANSITS 2105157298)) 100)) "% complete"))
    (println
      (str day ": " iteration-elapsed "ms, " (transits-per-second) " transits/sec"))
    (println
      (str "\tUpdate world (ms): " update-world-ms))
    (println
      (str "\tUpdate self loops (ms): " update-loops-ms))
    (println
      (str "\tUpdate visit starts (ms): " update-visits-ms))
    (println
      (str "\tCreate walkers (ms): " create-walkers-ms))
    (println
      (str "\tActive walkers: " (count (first current-walkers))))
    (println
      (str "\tRun walkers (ms): " run-walkers-ms))
      (println
        (str "\tUpdate Stats (ms): " update-stats-ms))
    (println
      (str "\tSpit results (ms): " log-results-ms))

  ) ; END MAIN RUN LOOP

  ; ... and print out final stats!
  (println (str "Days: " (count @DAYS)))
  (println (str "World size: " (count @WORLD)))
  (println (str "Unique edges: " (total-edges @WORLD)))
  (println (str "Total Walkers: " (count @LAST_VISITS)))
  (println (str "Transits: " @TRANSITS))
  (println (str "Run time (seconds): " (quot @ELAPSED_MS 1000.0)))
  (spit (clojure.string/join [(clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM]) "subreddit_counts.log"]) (json/write-str @SUBREDDIT_COUNTS))
  (def PAGE (atom 0))
  (def FILENAME_SUB_USER_COUNTS (atom (clojure.string/join [(clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM]) "subreddit_user_counts.log"])))
  (def FILENAME_SL_SUB_USER_COUNTS (atom (clojure.string/join [(clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM]) "self_loop_subreddit_user_counts.log"])))
  (def FILENAME_NSL_SUB_USER_COUNTS (atom (clojure.string/join [(clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM]) "non_self_loop_subreddit_user_counts.log"])))
  (dorun(map (fn [slice] 
    (swap! PAGE inc)
    (spit (clojure.string/join [@FILENAME_SUB_USER_COUNTS (str @PAGE)]) (json/write-str slice))) (partition-all 1000000 @s-u-c)))
  (def PAGE (atom 0))
  (dorun(map (fn [slice] 
    (swap! PAGE inc)
    (spit (clojure.string/join [@FILENAME_SL_SUB_USER_COUNTS (str @PAGE)]) (json/write-str slice))) (partition-all 1000000 @s-l-s-u-c)))
  (def PAGE (atom 0))
  (dorun(map (fn [slice] 
    (swap! PAGE inc)
    (spit (clojure.string/join [@FILENAME_NSL_SUB_USER_COUNTS (str @PAGE)]) (json/write-str slice))) (partition-all 1000000 @n-s-l-s-u-c)))
)
;(-main)