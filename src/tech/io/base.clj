(ns tech.io.base
  "Basic io that handles most simple cases"
  (:require [tech.io.protocols :as io-prot]
            [tech.io.url :as url]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import [java.io File InputStream OutputStream]
           [java.nio.file Files LinkOption]
           [java.util Date]))


(set! *warn-on-reflection* true)


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


(defn file->byte-length
  ^long [^File file]
  (-> (.toPath file)
      Files/size))


(defn parts->file
  [url-parts]
  (when-not (= :file (:protocol url-parts))
    (throw (ex-info "Not a file" url-parts)))
  (io/file (url/parts->file-path url-parts)))


(extend-protocol io-prot/IOProvider
  Object
  (input-stream [this url-parts options] (io-input-stream url-parts options))
  (output-stream! [this url-parts options] (io-output-stream url-parts options))
  (exists? [this url-parts options]
    (try
      (.close ^InputStream (io-prot/input-stream this url-parts options))
      true
      (catch Throwable e false)))
  (ls [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))
  (delete! [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))
  (metadata [provider url-parts options] {})

  File
  (input-stream [this url-parts options]
    (io/make-input-stream (parts->file url-parts) options))
  (output-stream! [this url-parts options]
    (let [fileme (parts->file url-parts)]
      (io/make-parents fileme)
      (io/make-output-stream fileme options)))
  (exists? [this url-parts options]
    (.exists ^File this))
  (ls [this url-parts options]
    (let [fileme (parts->file url-parts)]
      (->> (if (:recursive? options)
             (file-seq fileme)
             (fs/list-dir fileme))
           (map (fn [^File f]
                  {:url (str "file://" (.toString f))
                   :directory? (.isDirectory f)})))))
  (delete! [this url-parts options]
    (fs/delete-dir this))
  (metadata [provider url-parts options]
    {:modify-date (file->last-modify-time provider)
     :byte-length (file->byte-length provider)}))


(extend-protocol io-prot/ICopyObject
  Object
  (get-object [provider url-parts options]
    (io-prot/input-stream provider url-parts options))
  (put-object! [provider url-parts value options]
    (with-open [^InputStream in-s (io/input-stream value)
                ^OutputStream out-s (io-prot/output-stream! provider url-parts options)]
      (io/copy in-s out-s)))

  File
  (get-object [provider url-parts options] (parts->file url-parts))
  (put-object! [provider url-parts value options]
    (let [fileme (parts->file url-parts)]
      (io/make-parents fileme)
      ;;A short bit ot looking around makes it appear that stream copy
      ;;is fastest for files.
      (with-open [in-s (io/input-stream value)
                  out-s (io/output-stream fileme)]
        (io/copy in-s out-s)))))
