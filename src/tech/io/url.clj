(ns tech.io.url
  (:require [clojure.string :as s])
  (:import [java.net URL]
           [java.io File]))


(defn parse-url-arguments
  [args]
  (->> (s/split args #"&")
       (mapv (fn [arg-str]
               (let [eq-sign (.indexOf arg-str "=")]
                 (if (>= eq-sign 0)
                   [(.substring arg-str 0 eq-sign)
                    (.substring arg-str (+ eq-sign 1))]
                                arg-str))))))


(defn url->parts
  [url]
  (let [url (str url)
        [url args] (let [arg-delimiter (.indexOf url "?")]
                     (if (>= arg-delimiter 0)
                       [(.substring url 0 arg-delimiter)
                        (.substring url (+ arg-delimiter 1))]
                       [url nil]))
        parts (s/split url #"/")
        ^String protocol-part (first parts)
        path (drop 2 parts)
        args (when args (parse-url-arguments args))]
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
         "?"
         (s/join
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
  (s/join File/separator str-seq))


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
