(ns tech.io.url
  (:require [clojure.string :as s])
  (:import [java.net URL]
           [java.io File]))


(defn url->parts
  [url]
  (let [url (str url)
        [url args] (let [first-octothorpe (.indexOf url "#")]
                     (if (>= first-octothorpe 0)
                       [(.substring url 0 first-octothorpe)
                        (.substring url (+ first-octothorpe 1))]
                       [url nil]))
        parts (s/split url #"/")
        ^String protocol-part (first parts)
        path (drop 2 parts)]
    {:protocol (keyword (.substring protocol-part 0 (- (count protocol-part) 1)))
     :path path
     :arguments args}))


(defn- join-forward-slash
  ^String [path]
  (s/join "/" path))


(defn parts->url
  "Returns a string.  Not a java url."
  ^String [{:keys [protocol path arguments]}]
  (if arguments
    (str (name protocol) "://"
         (join-forward-slash path)
         "#"
         arguments)
    (str (name protocol) "://"
         (join-forward-slash path))))


(defn url?
  [url]
  (if (string? url)
    (try
      (:protocol (url->parts url))
      (catch Throwable e
        false))
    false))


(defn string-seq->file-path
  [str-seq]
  (s/join File/separator str-seq))


(defn parts->file-path
  [{:keys [protocol path arguments]}]
  ;;Windows will need something else here.
  (string-seq->file-path path))
