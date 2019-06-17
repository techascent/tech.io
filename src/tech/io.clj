(ns tech.io
  "Wrapper for tech io subsystem designed to be drop in replacement for some use cases
of clojure.java.io."
  (:require [clojure.java.io :as io]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [tech.io.edn :as edn]
            [tech.io.base]
            [tech.io.temp-file :as temp-file]
            [tech.resource :as resource]
            [tech.io.providers :as providers])
  (:import [javax.imageio ImageIO]
           [java.io InputStream OutputStream File]
           [java.awt.image BufferedImage]
           [java.nio.file Files Path StandardCopyOption
            CopyOption]))


(set! *warn-on-reflection* true)


(defn enable-s3!
  []
  (require 'tech.io.s3))


;;Purists or poeple using components will want to use the io-protocols directly with
;;providers passed in.  This API is meant to mimic clojure.java.io but in a more
;;extensible way.
(def ^:dynamic *provider-fn* #(or (providers/default-provider)
                                  (io-prot/url-parts->provider %)))


(defmacro with-provider-fn
  [provider-fn & body]
  `(with-bindings {#'*provider-fn* ~provider-fn }
     ~@body))


(defmacro with-provider
  [provider & body]
  `(with-provider-fn (constantly ~provider) ~@body))


(defn- args->map
  [args]
  (apply hash-map args))


;;Straight forwards
(def reader io/reader)
(def writer io/writer)
(def make-parents io/make-parents)


(defn file
  "Wrapper around "
  ^File [path-or-url]
  (let [filepath (if (url/url? path-or-url)
                   (-> (url/url->parts path-or-url)
                       url/parts->file-path)
                   path-or-url)]
    (io/file filepath)))


(defmacro ^:private lookup-provider
  [url & body]
  `(let [~'url-parts (url/url->parts ~url)
         ~'provider (*provider-fn* ~'url-parts)]
     ~@body))


(defn input-stream
  "thing->input-stream conversion.  Falls back to clojure.java.io if url is not a string url"
  ^InputStream [url & options]
  (if (url/url? url)
    (lookup-provider url
      (io-prot/input-stream provider url-parts (args->map options)))
    (apply io/input-stream url options)))


(defn output-stream!
  "thing->output-stream conversion.  Falls back to clojure.java.io if url is not a string url"
  ^OutputStream [url & options]
  (if (url/url? url)
    (lookup-provider url
      (io-prot/output-stream! provider url-parts (args->map options)))
    (apply io/output-stream url options)))


(defn copy
  [src dest & args]
  (with-open [^InputStream in-s (apply input-stream src args)
              ^OutputStream out-s (apply output-stream! dest args)]
    (io/copy in-s out-s)))


(defn interlocked-copy-to-file
  "Copy first to a temp, then do an atomic move to the destination.  This avoids
  issues with partial files showing up where they shouldn't and a failed io operation
  leading to incomplete results."
  [src dest & args]
  (resource/stack-resource-context
   (let [temp-fname (str dest ".tmp")
         _ (temp-file/watch-file-for-delete temp-fname)
         dest-file (file dest)]
     (with-open [^InputStream in-s (apply input-stream src args)
                 ^OutputStream out-s (apply output-stream! temp-fname args)]
       (io/copy in-s out-s))
     (let [^File src-file (file temp-fname)]
       (Files/move (.toPath src-file) (.toPath dest-file)
                   (into-array CopyOption
                               [StandardCopyOption/ATOMIC_MOVE
                                StandardCopyOption/REPLACE_EXISTING])))
     dest)))


(defn ls
  "Return a directory listing.  May be recursive if desired; only works with file
or s3 providers."
  [url & {:keys [recursive?] :as options}]
  (lookup-provider url
    (io-prot/ls provider url-parts
                (assoc (args->map options)
                       :recursive? recursive?))))


(defn delete!
  "Delete a resource.  Works currently with file or s3."
  [url & options]
  (lookup-provider url
    (io-prot/delete! provider url-parts (args->map options))))


(defn exists?
  "Boolean existence check.  Works with everything as fallback is to open
an input stream and then close it."
  [url & options]
  (lookup-provider url
    (io-prot/exists? provider url-parts (args->map options))))


(defn metadata
  "If supported, returns at least :modify-date and :byte-length.
Exception otherwise."
  [url & options]
  (lookup-provider url
    (io-prot/metadata provider url-parts options)))


(defn get-object
  "Get object always returns something convertible to an input-stream.
It may return a file for instance."
  [url & options]
  (lookup-provider url
    (io-prot/get-object provider url-parts (args->map options))))


(defn put-object!
  "Put object.  Object must be a byte-array, a file, or an input-stream.
Strings will be interpreted as per the rules of clojure.java.io/input-stream.
The most optimizations will apply to either files or byte arrays."
  [url value & options]
  (lookup-provider url
    (io-prot/put-object! provider url-parts value (args->map options))))


(defn put-edn!
  "Put edn data to a url"
  [url data & options]
  (lookup-provider url
    (edn/put-edn! provider url-parts data (args->map options))))


(defn get-edn
  "Get edn data from a url"
  [url & options]
  (lookup-provider url
    (edn/get-edn provider url-parts (args->map options))))


(defn put-nippy!
  "Put nippy data to a url"
  [url data & options]
  (lookup-provider url
    (edn/put-nippy! provider url-parts data (args->map options))))


(defn get-nippy
  "Get nippy data from a url"
  [url & options]
  (lookup-provider url
    (edn/get-nippy provider url-parts (args->map options))))


(defn put-image!
  "Will throw if an image with transparency is used to write a jpeg"
  [image path-or-url & {:as options}]
  (let [path-ext (url/extension path-or-url)]
    (with-open [out-s (output-stream! path-or-url)]
      (ImageIO/write ^BufferedImage image path-ext out-s))))


(defn get-image
  [path-or-url]
  (resource/stack-resource-context
    (let [temp (temp-file/watch-file-for-delete
                (temp-file/random-file-url))]
      (copy path-or-url temp)
      (ImageIO/read ^File (file temp)))))


(def ^:dynamic *enable-poor-api-naming?* true)


(defn enable-poor-api-naming!
  []
  (def output-stream output-stream!)
  (def put-object put-object!))


(when *enable-poor-api-naming?*
  (enable-poor-api-naming!))
