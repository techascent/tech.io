(ns tech.io.uuid
  (:import [java.util UUID]))


(defn random-uuid [] (UUID/randomUUID))

(defn random-uuid-str [] (str (random-uuid)))
