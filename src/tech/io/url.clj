(ns tech.io.url
  (:require [clojure.string :as string]
            [clojure.spec.alpha :as s])
  (:import [java.net URL]
           [java.io File]))


(set! *warn-on-reflection* true)


(defn parse-url-arguments
  [args]
  (->> (string/split args #"&")
       (mapv (fn [^String arg-str]
               (let [eq-sign (.indexOf arg-str "=")]
                 (if (>= eq-sign 0)
                   [(.substring arg-str 0 eq-sign)
                    (.substring arg-str (+ eq-sign 1))]
                                arg-str))))))


(defn url->parts
  "It is not a great idea to add custom java url protocls as it involves creating
  a new stream handler and that is a one-off (per-program) operation thus you would
  potentially conflict with anyone else who did such a thing:
  https://stackoverflow.com/questions/26363573/registering-and-using-a-custom-java-net-url-protocol"
  [url]
  (let [url (str url)
        [url args] (let [arg-delimiter (.indexOf url "?")]
                     (if (>= arg-delimiter 0)
                       [(.substring url 0 arg-delimiter)
                        (.substring url (+ arg-delimiter 1))]
                       [url nil]))
        parts (string/split url #"/")
        ^String protocol-part (first parts)
        parts (rest parts)
        path (if (= 0 (count (first parts)))
                (rest parts)
                (throw (ex-info (format "Unrecognized url: %s" url)
                                {:url url})))
        args (when args (parse-url-arguments args))]
    {:protocol (keyword (.substring protocol-part 0 (- (count protocol-part) 1)))
     :path path
     :arguments args}))


(defn- join-forward-slash
  ^String [path]
  (string/join "/" path))


(defn parts->url
  "Returns a string.  Not a java url."
  ^String [{:keys [protocol path arguments]}]
  (if arguments
    (str (name protocol) "://"
         (join-forward-slash path)
         "?"
         (string/join
          "&"
          (->> arguments
               (map (fn [arg]
                      (if (= 2 (count arg))
                        (str (first arg) "=" (second arg))
                        arg))))))
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
  (string/join File/separator str-seq))


(defn parts->file-path
  [{:keys [protocol path arguments] :as url-parts}]
  (when-not (= protocol :file)
    (throw (ex-info "Not a file url" url-parts)))
  (string-seq->file-path path))


(defn url->file-path
  [url]
  (-> (url->parts url)
      parts->file-path))


(defn extension
  ^String [^String str-url]
  (.substring str-url (+ 1 (.lastIndexOf str-url "."))))
