(ns machinery.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv])
  (:gen-class))


;; Configuration derpage

(def ROOT_NODE :reddit.com) ; always available from any sub
(def BATCH_SIZE 1000) ; defines the batch size for walking


;; STATE -----------------------------------------------------------------------

(def WORLD (atom {}))
(def SELF_LOOP_PCT (atom {}))
(def DAYS (atom []))
(def LAST_VISITS (atom {}))
(def TRANSITS (atom 0))
(def ELAPSED_MS (atom 0))


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

(defn initialize-world!
  "Reads in original state for the world"
  []
  (let [world-src (slurp-json "../data/initial_net.json")]

    ; ensure that all neighbors are also top level nodes with the correct data structure -- {:subreddit []}
    (doseq [sub (distinct (flatten (vals world-src)))]
      (if (not (contains? @WORLD (keyword sub)))
        (swap! WORLD assoc (keyword sub) [ROOT_NODE])))

    ; ensure that all of the outbound neighbors are keywords
    (doseq [sub (keys world-src)]
      (swap! WORLD assoc sub (map keyword (get world-src sub))))

    ; ensure that all neighbor lists can exit back to ROOT_NODE
    (doseq [sub (keys world-src)]
      (if (empty? (get @WORLD sub))
        (swap! WORLD assoc (keyword sub) [ROOT_NODE])))))

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
  (slurp-csv (str "../larger_data/edge_creation/" day)))

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
  (let [self-loops (slurp-csv-kv (str "../larger_data/self_loop_percents/" day))]
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
      (file-seq (clojure.java.io/file "../larger_data/edge_creation")))))


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
    (for [[username subreddit] (slurp-csv (str "../larger_data/user_starts/" day))]
      [(keyword username) (keyword subreddit)])))

(defn update-last-visits!
  "Uses the user starting data to seed LAST_VISITS"
  [day]
  (swap! LAST_VISITS merge (slurp-user-starts day)))


;; WALKERS CRUD ----------------------------------------------------------------

(defn slurp-user-counts
  "Creates [[username count] ...] from file"
  [day]
  (slurp-csv (str "../larger_data/user_counts/" day)))

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
          (recur (conj history (walk first-step)))
          (recur (conj history (walk (last history)))))))))

(defn run-and-measure-walk
  [walker-pair]
  (let [[username total-steps] walker-pair
        history (random-walk username total-steps)]
    (set-last-visit (keyword username) (last history))
    (swap! TRANSITS + (count history)))
  true)

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

  ; Set up the initial state of the universe
  (reset! DAYS (initial-days))
  (initialize-world!)
  (reset! SELF_LOOP_PCT (initial-self-loop-pct @WORLD))

  ; BEGIN MAIN RUN LOOP
  (doseq [day @DAYS]
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
        (dorun ; force realization
          (pmap ; executes each of the run-batch functions in parallel
            run-batch
              (create-batches current-walkers)))))

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

  ) ; END MAIN RUN LOOP

  ; ... and print out final stats!
  (println (str "Days: " (count @DAYS)))
  (println (str "World size: " (count @WORLD)))
  (println (str "Unique edges: " (total-edges @WORLD)))
  (println (str "Total Walkers: " (count @LAST_VISITS)))
  (println (str "Transits: " @TRANSITS))
  (println (str "Run time (seconds): " (quot @ELAPSED_MS 1000.0)))
)
