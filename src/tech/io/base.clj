(ns tech.io.base
  "Basic io that handles most simple cases"
  (:require [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import [java.io File]
           [java.nio.file Files LinkOption]
           [java.util Date]))


(defn- io-input-stream
  [url-parts options]
  (io/make-input-stream (url/parts->url url-parts) options))


(defn- io-output-stream
  [url-parts options]
  (io/make-output-stream (url/parts->url url-parts) options))


(defn- file->last-modify-time
  [^File file]
  (-> (Files/getLastModifiedTime (.toPath file)
                                 (make-array LinkOption 0))
      (.toMillis)
      (Date.)))


(defn- file->byte-length
  ^long [^File file]
  (-> (.toPath file)
      Files/size))


(extend-protocol io-prot/IOProvider
  Object
  (input-stream [this url-parts options] (io-input-stream url-parts options))
  (output-stream! [this url-parts options] (io-output-stream url-parts options))
  (exists? [this url-parts options]
    (try
      (.close (io-prot/input-stream this url-parts options))
      true
      (catch Throwable e false)))
  (ls [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))
  (delete! [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))
  (metadata [provider url-parts options] {})

  File
  (input-stream [this url-parts options]
    (io/make-input-stream this options))
  (output-stream! [this url-parts options]
    (io/make-parents this)
    (io/make-output-stream this options))
  (exists? [this url-parts options]
    (.exists ^File this))
  (ls [this url-parts options]
    (->> (fs/list-dir this)
         (map (fn [^File f]
                {:url (str "file://" (.toString f))
                 :directory? (.isDirectory f)}))))
  (delete! [this url-parts options]
    (fs/delete-dir this))
  (metadata [provider url-parts options]
    {:modify-date (file->last-modify-time provider)
     :byte-length (file->byte-length provider)}))


(defmethod io-prot/url-parts->provider :default
  [& args]
  (Object.))


(defmethod io-prot/url-parts->provider :file
  [url-parts]
  (io/file (url/parts->file-path url-parts)))
