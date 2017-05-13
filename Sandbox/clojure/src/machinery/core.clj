(ns machinery.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv])
  (:gen-class))



(def WORLD (atom {}))
(def SELF_LOOP_PCT (atom {}))
(def DAYS (atom []))
(def LAST_VISITS (atom {}))
(def TRANSITS (atom 0))
(def ELAPSED_MS (atom 0))

(def ROOT_NODE :reddit.com)
(def BATCH_SIZE 50) ; defines the batch size for walking

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
  (let [world (slurp-json "../data/initial_net.json")]
    ; "world" is reasonably complete, but needs some cleanup.
    (reset! WORLD world)

    ; let's ensure that all subs have the correct data structure -- {:subreddit []}
    (doseq [sub (distinct (flatten (vals @WORLD)))]
      (if (not (contains? @WORLD (keyword sub)))
        (swap! WORLD assoc (keyword sub) [ROOT_NODE])))

    ; now, let's ensure that all of the outbound neighbors are keywords
    (doseq [sub (keys @WORLD)]
      (swap! WORLD assoc sub (map keyword (get @WORLD sub))))))

(defn ensure-node
  "Ensures a key with a default value exists in a hashmap if it doesn't already"
  [m key]
  (if (contains? m key)
    m
    (assoc m key [ROOT_NODE])))

(defn add-edge
  "Ensures the origin and destination nodes exists, and adds an edge"
  [world edge-pair]
  (let [[origin destination] edge-pair
        new-world            (-> world (ensure-node origin) (ensure-node destination))
        original-edges       (get new-world origin)
        new-edges            (set (conj original-edges destination))]
        (assoc new-world origin new-edges)))

(defn build-updated-world
  "Builds an updated copy of the world"
  [world day]
  (loop [edges (slurp-csv (str "../larger_data/edge_creation/" day))
         new-world world]
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
  (/ (rand) 1000))

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

(defn create-walker
  "Creates a walker without a :current-node"
  [username transit-count]
  {:username (keyword username) :transit-count (read-string transit-count)})

(defn create-walkers
  "Returns a list of walkers for a given day."
  [day]
  (map #(apply create-walker %) (slurp-user-counts day)))


;; WALKING LOGIC ---------------------------------------------------------------

(defn stay-on-current-node?
  "Determines if the walker should stay on the current node"
  [current-node]
  ; first of all, if the data's broke, fix it!
  (if-not (contains? @SELF_LOOP_PCT current-node)
    (ensure-self-loop-pct! current-node))
  (> (get @SELF_LOOP_PCT current-node) (rand)))

(defn random-neighbor
  "Selects a random neighbor"
  [current-node]
  (let [neighbors (-> @WORLD current-node)]
    (rand-nth neighbors)))

(defn walk
  "Performs a single traverse"
  [current-node]
  (if (stay-on-current-node? current-node)
    current-node
    (random-neighbor current-node)))

(defn random-walk
  "Performs the random walk"
  [walker]
  (let [username (:username walker)
        total-steps (:transit-count walker)
        first-step (get @LAST_VISITS (keyword username))]

    (loop [history []]
      (if (= (count history) total-steps) ; we have as much history as steps
        history
        (if (empty? history)
          (recur (conj history (walk first-step)))
          (recur (conj history (walk (last history)))))))))


(defn millis
  []
  (System/currentTimeMillis))

(defn total-edges
  [world]
  (loop [edge-count 0
         node-keys (keys world)]
    (if (empty? node-keys)
      edge-count
      (recur (+ edge-count (count (get world (first node-keys))))
             (rest node-keys)))))

(defn run-and-measure-walk
  [walker]
  (let [history (random-walk walker)]
    (set-last-visit (:username walker) (last history))
    (swap! TRANSITS + (count history)))
  true)

(defn run-batch
  [walkers]
  (doall (map run-and-measure-walk walkers)))

(defn split-up
  [walkers]
  (partition BATCH_SIZE BATCH_SIZE [] walkers))

(defn -main
  [& args]
  (reset! DAYS (initial-days))
  (initialize-world!)
  (reset! SELF_LOOP_PCT (initial-self-loop-pct @WORLD))

  (try
    (doseq [day @DAYS]
      (println day)

      (def start-time (millis))

          (update-world! day)
          (update-self-loop-pct! day)
          (update-last-visits! day)

          (let [walkers (create-walkers day)]
            (dorun
              (pmap run-batch (split-up walkers))))


      (def local-elapsed (- (millis) start-time))
      (swap! ELAPSED_MS + local-elapsed)

      (println (str "Iteration: " local-elapsed "ms, " (* 1000 (quot @TRANSITS @ELAPSED_MS)) " transits/sec for " (quot @ELAPSED_MS 1000) " seconds" )))

  (catch Exception e
    (println (str "caught exception: " (.getMessage e)))))

  (println (str "World size: " (count @WORLD)))
  (println (str "Unique edges: " (total-edges @WORLD)))
  (println (str "Total Walkers: " (count @LAST_VISITS)))
  (println (str "Transits: " @TRANSITS))
)
