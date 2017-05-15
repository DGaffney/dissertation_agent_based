(ns machinery.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
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

(def ROOT_NODE :reddit.com) ; always available from any sub
(def BATCH_SIZE 1000) ; defines the batch size for walking

;; STATE -----------------------------------------------------------------------

(def WORLD (atom {}))
(def SELF_LOOP_PCT (atom {}))
(def SUBREDDIT_COUNTS (atom {}))
(def SUBREDDIT_USER_COUNTS (atom {}))
(def DAYS (atom []))
(def RANDOM_WALK_ALGORITHM (atom "random-walk"))
(def FILEPATH (atom "../larger_data"))
(def LAST_VISITS (atom {}))
(def TRANSITS (atom 0))
(def ELAPSED_MS (atom 0))
(def SIMULATION_ID (int (* (rand) 10000000)))
(def FILENAME (atom @RANDOM_WALK_ALGORITHM))
(def HISTORIES (atom []))
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
  [m key]
  (let [kw (keyword key)] ; endure we're dealing with a keyword
    (if (contains? m kw)
      m
      (assoc m kw [ROOT_NODE]))))

(defn add-edge
  "Ensures the origin and destination nodes exists, and adds an edge"
  [world edge-pair]
  (let [[origin destination] edge-pair
        new-world            (-> world (ensure-node origin) (ensure-node destination))
        original-edges       (get new-world origin)
        new-edges            (set (conj original-edges destination))] ; set ensures all values are unique
        (assoc new-world origin new-edges)))

(defn slurp-edges
  "Loads raw edges from disk"
  [day]
  (slurp-csv (str (clojure.string/join [@FILEPATH "/edge_creation/"]) day)))

(defn build-updated-world
  "Builds an updated copy of the world"
  [world day]
  (loop [edges      (slurp-edges day) ; start with raw list of edges for the day
         new-world  world]
    (if (empty? edges)
      new-world ; return the new world with all of it's edges
      (recur (rest edges)
             (add-edge new-world (first edges))))))

(defn update-world!
  "Updates the world with new edges"
  [day]
  (swap! WORLD merge (build-updated-world @WORLD day)))

;(defn log-day
;  "Spit the results of the day into file"
;  [day]
;  (log/info (clojure.string/join [(clojure.string/join ["==================" day "=================="]) "\n" (str @HISTORIES) "\n"]))
;  (reset! HISTORIES []))
(defn log-day
  [day]
  (info (clojure.string/join [(clojure.string/join ["==================" day "=================="]) "\n" @HISTORIES "\n"]))
  (reset! HISTORIES []))

;; STATS CRUD ------------------------------------------------------------------

(defn rand-self-loop-pct
  []
  (/ (rand) 1000.0))

(defn ensure-self-loop-pct!
  "Ensures a record exists in SELF_LOOP_PCT"
  [key]
  (if-not (contains? @SELF_LOOP_PCT key)
    (swap! SELF_LOOP_PCT assoc key (rand-self-loop-pct))))

(defn update-self-loop-pct!
  "Updates stats to inform walkers"
  [day]
  (let [self-loops (slurp-csv-kv (str (clojure.string/join [@FILEPATH "/self_loop_percents/"]) day))]
    (doseq [[subreddit value] self-loops]
      (swap! SELF_LOOP_PCT assoc subreddit (read-string value)))))

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

(defn slurp-user-counts
  "Creates [[username count] ...] from file"
  [day]
  (slurp-csv (str (clojure.string/join [@FILEPATH "/user_counts/"]) day)))

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
  (if (stay-on-current-node? current-node)
    current-node
    (-> @WORLD current-node rand-nth)))

(defn random-walk
  "Performs the random walk"
  [username raw-steps]
  (let [total-steps (read-string raw-steps) ; bah, comes in as a string
        first-step  (get @LAST_VISITS (keyword username))]
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

(defn update-subreddit-counts
  [history-frequencies]
  (let [[subreddit subreddit-count] history-frequencies]
    (swap! SUBREDDIT_COUNTS update-in [subreddit] (fnil (partial + subreddit-count) 0))))

(defn run-and-measure-walk
  [walker-pair]
  (let [[username total-steps] walker-pair
        history (run-random-walk username total-steps)
        history-frequencies (frequencies history)]
    (set-last-visit (keyword username) (last history))
    (pmap update-subreddit-counts history-frequencies)
    (pmap (fn [history-frequencies] (let [[subreddit subreddit-count] history-frequencies]
      (swap! SUBREDDIT_USER_COUNTS update-in [username subreddit] (fnil (partial + subreddit-count) 0)))) history-frequencies)
    (swap! TRANSITS + (count history))
  (swap! HISTORIES conj [username history])
  [username history]))

(defn run-batch
  [walkers]
  (doall ; force the map to execute
    (map run-and-measure-walk walkers)))

(defn create-batches
  [walkers]
  (partition BATCH_SIZE BATCH_SIZE [] walkers))


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
  (timbre/swap-config! assoc-in [:appenders :async?] true)
  ; BEGIN MAIN RUN LOOP
  (doseq [day (sort @DAYS)]
    ; timestamp the start of this iteration
    (println day)
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
        (dorun ; force realization
          (pmap ; executes each of the run-batch functions in parallel
            run-batch
              (create-batches current-walkers)))))

    (def log-results-ms
      (bench
        (log-day day)))
    ; total time (in ms) for executing this iteration of the simulation
    (def iteration-elapsed (- (millis) iteration-start-ms))

    ; record the execution time
    (swap! ELAPSED_MS + iteration-elapsed)

    ; status strings
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
      (str "\tActive walkers: " (count current-walkers)))
    (println
      (str "\tRun walkers (ms): " run-walkers-ms))
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
)
