(ns tech.v3.io.uuid
  "Utility for working with uuids"
  (:import [java.util UUID])
  (:refer-clojure :exclude [random-uuid]))


(defn random-uuid [] (UUID/randomUUID))

(defn random-uuid-str [] (str (random-uuid)))
