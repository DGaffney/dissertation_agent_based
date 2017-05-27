(ns machinery.core
  (:require [clojure.data.json :as json]
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
    :default "random-walk"]
    ["-p" "--path PATH" "File path for observed data" :default "../larger_data"]])

(def BATCH_SIZE 50) ; defines the batch size for walking
(def CORE_COUNT 12) ; number of CPU cores

;; STATE -----------------------------------------------------------------------
(def FUCKUPS (atom 0))
(def WORLD (atom {}))
(def SELF_LOOP_PCT (ref {}))
(def SUBREDDIT_COUNTS (ref {}))
(def SUBREDDIT_USER_COUNTS (ref {}))
(def DAYS (atom []))
(def RANDOM_WALK_ALGORITHM (atom "random-walk"))
(def FILEPATH (atom "../larger_data"))
(def LAST_VISITS (atom {}))
(def TRANSITS (ref 0))
(def ELAPSED_MS (atom 0))
(def SIMULATION_ID (int (* (rand) 10000000)))
(def FILENAME (atom @RANDOM_WALK_ALGORITHM))
(def HISTORIES (ref []))

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

;(defn build-updated-world
;  "Builds an updated copy of the world"
 ; [world day]
 ; (loop [edges      (slurp-edges day) ; start with raw list of edges for the day
;         edge-map (slurp-edges-map day)
;         new-world (merge world (zipmap (new-nodes (keys edge-map)) (repeat [])))]
;    (if (empty? edges)
;      new-world ; return the new world with all of it's edges
;      (recur (rest edges)
;             (add-edge new-world (first edges))))))

(defn read-world
  [day]
  (json/read-str (slurp (str (clojure.string/join [@FILEPATH "/worlds/"]) day))))

;(defn update-world!
;  "Updates the world with new edges"
;  [day]
;  (swap! WORLD merge (build-updated-world @WORLD day)))
  
(defn update-world!
  [day]
  (swap! WORLD #(merge-with concat % (slurp-edges-map day))))


;(defn agent-write [w content]
;  (doto w
;    (.write w content)
;    (.write "\n")
;    .flush))

;(defn log-agent [day] (agent (io/writer (daily-log-file day) :append true)))
;(defn log-file [day] (agent (io/writer (daily-log-file day))))

;(defn daily-log-folder-path [day]
;  (clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM "daily_results"]))

;(defn daily-log-file [day]
;  (clojure.string/join "/" [(daily-log-folder-path day) day]))

;(defn log-day
;  [day]
;  (let [folder_path (clojure.string/join "_" [(str SIMULATION_ID) @RANDOM_WALK_ALGORITHM "daily_results"])
;  history-snap @HISTORIES]
;  (io/make-parents (daily-log-file day))
;  (send (log-agent day) agent-write history-snap)
;  (def HISTORIES (ref []))))
(defn log-day
  [day]
  (info (clojure.string/join [(clojure.string/join ["==================" day "=================="]) "\n" @HISTORIES "\n"]))
  (def HISTORIES (ref [])))

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
  [username subreddit]
  (swap! LAST_VISITS assoc (keyword username) (keyword subreddit)))

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
  (swap! LAST_VISITS merge (slurp-user-starts day)))


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
        parsed-csv-data (map cast-user-count-row csv-data)
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
    (keyword current-node)
    (let [step (-> @WORLD current-node rand-nth)]
      (if (= nil step) current-node step))))

(defn random-walk
  "Performs the random walk"
  [username total-steps]
  (let [first-step  (get @LAST_VISITS (keyword username))]
    (loop [history []]
      (if (= (count history) total-steps) ; we have as much history as steps
        history
        (if (empty? history)
          (recur (conj history first-step))
          (recur (conj history (walk (last history)))))))))

(defn run-random-walk
  [username total-steps]
  (cond 
    (= @RANDOM_WALK_ALGORITHM "random-walk") (random-walk username total-steps)))

(defn run-and-measure-walk
  [walker-pair]
  (let [[username total-steps] walker-pair
        history (run-random-walk username total-steps)
        history-frequencies (frequencies history)]
    (set-last-visit (keyword username) (last history))
;    (pmap update-subreddit-counts history-frequencies)
    (map (fn [history-frequencies] (let [[subreddit subreddit-count] history-frequencies]
      (swap! SUBREDDIT_USER_COUNTS update-in [username subreddit] (fnil (partial + subreddit-count) 0)))) history-frequencies)
;    (swap! TRANSITS + (count history))
;  (swap! HISTORIES conj [username history])
  [username history]))

(defn update-subreddit-counts
  [subreddit-counts]
  (map (fn [subreddit-count] 
    (dosync (commute SUBREDDIT_COUNTS update-in [(first subreddit-count)] (fnil (partial + (last subreddit-count)) 0)))) subreddit-counts))
  
(defn update-subreddit-user-counts
  [subreddit-user-counts]
  (map (fn [subreddit-user-count]
    (map (fn [history-frequency] (let [[subreddit subreddit-count] history-frequency]
      (dosync (commute SUBREDDIT_COUNTS update-in [(first subreddit-user-count) subreddit] (fnil (partial + subreddit-count) 0))))) (into [] (last subreddit-user-count)))) subreddit-user-counts))

(defn update-stats
  [walker-results]
  (let [transposed-results (apply map list walker-results)
        history-set (nth transposed-results 0)
        subreddit-count-set (nth transposed-results 1)
        subreddit-counts (apply merge-with + subreddit-count-set)
        subreddit-user-count-set (nth transposed-results 2)
        subreddit-user-counts (apply merge-with + subreddit-user-count-set)
        transit-set (nth transposed-results 3)]
    (dosync (alter HISTORIES conj history-set))
    (update-subreddit-counts subreddit-counts)
    ;(update-subreddit-user-counts subreddit-user-counts)
    (dosync (alter TRANSITS + (reduce + transit-set)))))

;(defn run-batch
;  [walkers]
;  (let [post-histories (doall (map run-and-measure-walk walkers))
;        subreddit-counts (frequencies (flatten (map last post-histories)))
;        transit-count (reduce + (vals subreddit-counts))]
;    (swap! TRANSITS + transit-count)
;    (pmap (fn [subreddit-count-pair] (let [[subreddit subreddit-count] subreddit-count-pair] 
;      (swap! SUBREDDIT_COUNTS update-in [subreddit] (fnil (partial + subreddit-count) 0)))) subreddit-counts)
;    (pmap (fn [user-history] 
;      (let [[username history] user-history
;            history-freq (frequencies history)] 
;      (swap! HISTORIES conj [username history])
;      (pmap (fn [subreddit] 
;        (swap! SUBREDDIT_USER_COUNTS update-in [username subreddit] (fnil (partial + (get history-freq subreddit)) 0))) (keys history-freq))
;      )) post-histories)))  
;(reduce + (flatten (map vals (vals @SUBREDDIT_USER_COUNTS))))

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

; (defn create-batches
;   [walker-data]
;   (let [walkers  (first walker-data)
;         walk-sum (last walker-data)
;         current-count 0
;         target-count (int (/ walk-sum CORE_COUNT))
;         current-batch []
;         batches []]
; ))
;
;   (partition-all BATCH_SIZE walkers))
(defn run-batch
  [walkers]
  (let [histories (doall (map run-and-measure-walk walkers))
        subreddit-counts (frequencies (flatten (map last histories)))
        subreddit-user-counts (into {} (map (fn [pair] [(first pair) (frequencies (last pair))]) histories))
        transits (reduce + (vals subreddit-counts))]
    [histories subreddit-counts subreddit-user-counts transits]))



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
  (doseq [day (take 500 (sort @DAYS))]
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
  (dorun(map (fn [slice] 
    (swap! PAGE inc)
    (spit (clojure.string/join [@FILENAME_SUB_USER_COUNTS (str @PAGE)]) (json/write-str slice))) (partition-all 1000000 @SUBREDDIT_USER_COUNTS)))
)