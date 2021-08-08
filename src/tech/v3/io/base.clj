(ns tech.v3.io.base
  "Basic io that handles most simple cases"
  (:require [tech.v3.io.protocols :as io-prot]
            [tech.v3.io.url :as url]
            [clojure.java.io :as io]
            [babashka.fs :as fs])
  (:import [java.io File InputStream OutputStream]
           [java.nio.file Files LinkOption Path]
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
  ^File [url-parts]
  (when-not (= :file (:protocol url-parts))
    (throw (ex-info "Not a file" url-parts)))
  (io/file (url/parts->file-path url-parts)))


(defn unchecked-parts->file
  ^File [url-parts]
  (File. (url/parts->file-path (assoc url-parts :protocol :file))))


(defn- file-ls
  [path options]
  (let [path (.toString ^Object path)]
    (if (:recursive? options)
      (->> (file-seq (File. path))
           (map (fn [^Path f]
                  (io-prot/metadata (File. (.toString f))
                                    {:protocol :file
                                     :path [(.toString f)]}
                                    nil))))
      (let [path-meta (io-prot/metadata (File. path) {:protocol :file
                                                      :path [path]}
                                        options)]
        (if (:directory? path-meta)
          (let [file-data (File. path)]
            (->> (concat [file-data]
                         (.listFiles file-data))
                 (map #(io-prot/metadata % {:protocol :file
                                            :path [(.toString ^Object %)]}
                                         nil))))
          [path-meta])))))


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
    (let [url-parts (update url-parts :protocol #(if % % :file))]
      (io-prot/ls (unchecked-parts->file url-parts) url-parts options)))
  (delete! [this url-parts options]
    (let [url-parts (update url-parts :protocol #(if % % :file))]
      (fs/delete-tree (unchecked-parts->file url-parts) url-parts options)))
  (metadata [provider url-parts options]
    (let [url-parts (update url-parts :protocol #(if % % :file))]
      (io-prot/metadata (unchecked-parts->file url-parts) url-parts options)))
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
    (file-ls (parts->file url-parts) options))
  (delete! [this url-parts options]
    (fs/delete-tree this))
  (metadata [provider url-parts options]
    (let [f (unchecked-parts->file url-parts)]
      {:modify-date (file->last-modify-time f)
       :byte-length (file->byte-length f)
       :directory? (.isDirectory f)
       :url (str "file://" (.toString f))})))


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


(defmethod io-prot/url-parts->provider :default
  [& args]
  (Object.))


(defmethod io-prot/url-parts->provider :file
  [url-parts]
  (parts->file url-parts))
