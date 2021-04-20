(ns tech.v3.io.temp-file
  (:require [tech.v3.resource :as resource]
            [babashka.fs :as fs]
            [clojure.string :as s]
            [tech.v3.io.uuid :as uuid]
            [tech.v3.io.url :as url])
  (:import [java.io File]
           [java.nio.file Paths]))


;; We have had issues where we filled up the temporary space on the volume
;; of the docker image in production with temp files.  To avoid this there is
;; an implicit assumption that ring-produced temporary files will be deleted
;; once the handler is finished.  While this isn't a perfect solution it could
;; be extended and used in other places and then we would have a lot more confidence
;; that temporary data would be cleaned up regardless of failure conditions

(defn- combine-paths
  [& args]
    (->
     (Paths/get (first args) (into-array String (rest args)))
     (.toString)))

(defn- random-temp-dir-str
  ^String [root]
  (combine-paths root (uuid/random-uuid-str)))


(defonce ^:dynamic *files-in-flight* (atom #{}))


(defn watch-file-for-delete
  [path-or-file]
  (resource/track
   #(let [path-or-file
          (if (url/url? path-or-file)
            (url/parts->file-path (url/url->parts path-or-file))
            path-or-file)]
      (fs/delete-tree path-or-file))
   {:track-type :stack})
  path-or-file)


(defn system-temp-dir
  []
  (System/getProperty "java.io.tmpdir"))


(defn random-temp-dir
  [& {:keys [root]
      :or {root (system-temp-dir)}
      :as options}]
  (let [retval (random-temp-dir-str root)]
    (fs/create-dirs retval options)
    (watch-file-for-delete retval)))


(defmacro with-temp-dir
  "Execute code with a variable bound to the name of a temp directory
that will be removed when the code completes (or throws an exception)."
  [dirname-var & body]
  `(resource/stack-resource-context
     (let [~dirname-var (random-temp-dir)]
       ~@body)))


(defn random-file-url
  [& {:keys  [dirname suffix]}]
  (url/parts->url
   {:protocol :file
    :path (s/split (combine-paths (or dirname (system-temp-dir))
                                  (format "%s%s" (uuid/random-uuid-str)
                                          (or suffix "")))
                   (re-pattern "/\\\\"))}))


(defn temp-resource-file
  "Get a temp file location that will be delete when the resource context unwinds"
  [& {:keys [dirname suffix]}]
  (-> (random-file-url :dirname dirname :suffix suffix)
      (watch-file-for-delete)))
