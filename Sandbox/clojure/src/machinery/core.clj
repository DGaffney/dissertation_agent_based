(ns machinery.core
  (:gen-class))

(def world (atom {}))
(def stats (atom {}))
(def last_visit (atom {}))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
