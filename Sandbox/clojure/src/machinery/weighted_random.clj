(defn random-bigint [limit]
  (.nextLong (java.util.concurrent.ThreadLocalRandom/current) limit))

;original
(def w (reductions #(+ % %2) (vals m)))
(defn weighted-rand-choice [m]
    (let [w (reductions #(+ % %2) (vals m))
          r (random-bigint (last w))]
         (nth (keys m) (count (take-while #( <= % r ) w)))))

 ;faster?
(defn weighted-rand-choice [m w]
   (let [r (random-bigint (last w))]
        (java.util.Collections/binarySearch w r compare))

(require '[clojure.data.generators :as generators])

(def subreddit-counts {:reddit.com 10000 :blah 1000 :other 100 :nextone 10 :lastone 1})
(bench (generators/weighted subreddit-counts))
(bench (->> #(clojure.data.generators/weighted subreddit-counts) repeatedly (take 1000) frequencies))
(def w (reductions #(+ % %2) (vals subreddit-counts)))
(bench (weighted-rand-choice subreddit-counts w))




(->> #(weighted-rand-choice @SUBREDDIT_COUNTS) repeatedly (take 10000) frequencies)

(def characters ["A" "B" "C" "D" "E" "F" "G" "H" "I" "J" "K" "L" "M" "N" "O" "P" "Q" "R" "S" "T" "U" "V" "W" "X" "Y" "Z" "a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p" "q" "r" "s" "t" "u" "v" "w" "x" "y" "z" "0" "1" "2" "3" "4" "5" "6" "7" "8" "9"])
(defn rand-string [n]
  (->> (fn [] (rand-nth characters))
       repeatedly
       (take n)
       (apply str)))
(map (repeat ) (range 400))
(dotimes [n 5])
(defn gen-sub
[]
[(keyword (rand-string (rand 30))) (bigint (rand 500000))])

(def subreddit-counts (into {} (take 500000 (repeatedly #(gen-sub)))))
(bench (->> #(weighted-rand-choice subreddit-counts w) repeatedly (take 10) frequencies))



(ns binary-search)

(defn middle [coll]
  (->
    coll
    (count)
    (quot 2)))

(defn search-for [value coll]
  (let [middle (middle coll)
        current-value (nth coll middle)]
  (cond
    (= current-value value) middle
    (or (= middle (count coll)) (zero? middle))
      (throw (Exception. (format "%s not found." value)))
    (< current-value value) (+ middle (search-for value (drop middle coll)))
    (> current-value value) (search-for value (take middle coll)))))

(defn abs [x] (if (neg? x) (- x) x))
(defn find-closest [sm k]
  (if-let [a (key (first (rsubseq sm <= k)))]
    (if (= a k)
      a
      (if-let [b (key (first (subseq sm >= k)))]
        (if (< (abs (- k b)) (abs (- k a)))
          b
          a)))
    (key (first (subseq sm >= k)))))

(find-closest m 4)
(def m (apply map list [(keys subreddit-counts) (reductions #(+ % %2) (vals subreddit-counts))]))
(into {} (apply map list [(keys subreddit-counts) (reductions #(+ % %2) (vals subreddit-counts))]))
