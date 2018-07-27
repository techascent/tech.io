(ns tech.io.uuid
  "Utility for working with uuids"
  (:import [java.util UUID]))


(defn random-uuid [] (UUID/randomUUID))

(defn random-uuid-str [] (str (random-uuid)))
