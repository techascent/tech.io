(ns tech.io
  "Wrapper for tech io subsystem designed to be drop in replacement for some use cases
of clojure.java.io."
  (:require [clojure.java.io :as io]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [tech.io.edn :as edn]
            [tech.io.base]
            [tech.config.core :as config]))

;;Purists or poeple using components will want to use the io-protocols directly with providers
;;passed in.  This API is meant to mimic clojure.java.io but in a more extensible way.
(def ^:dynamic *provider* (when (config/get-config :tech-io-cache-local)
                            (require 'tech.io.cache)
                            ((resolve 'tech.io.cache/create-file-cache)
                             (config/get-config :tech-io-cache-dir)
                             {})))

(defmacro with-provider
  [url & body]
  `(let [~'url-parts (url/url->parts ~url)
         ~'provider (if *provider*
                      *provider*
                      (io-prot/url-parts->provider ~'url-parts))]
     ~@body))


(defn- args->map
  [args]
  (apply hash-map args))


(defn input-stream
  "thing->input-stream conversion.  Falls back to clojure.java.io if url is not a string url"
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/input-stream provider url-parts (args->map options)))
    (apply io/input-stream url options)))


(defn output-stream!
  "thing->output-stream conversion.  Falls back to clojure.java.io if url is not a string url"
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/output-stream! provider url-parts (args->map options)))
    (apply io/output-stream url options)))


(defn get-object
  "Get object always returns something convertible to an input-stream.
It may return a file for instance."
  [url & options]
  (with-provider url
    (io-prot/get-object provider url-parts (args->map options))))


(defn put-object!
  "Put object.  Object must be a byte-array, a file, or an input-stream.
Strings will be interpreted as per the rules of clojure.java.io/input-stream.
The most optimizations will apply to either files or byte arrays."
  [url value & options]
  (with-provider url
    (io-prot/put-object! provider url-parts value (args->map options))))


(defn ls
  "Return a directory listing.  May be recursive if desired; only works with file
or s3 providers."
  [url & {:keys [recursive?] :as options}]
  (with-provider url
    (io-prot/ls provider url-parts
                (assoc (args->map options)
                       :recursive? recursive?))))


(defn delete!
  "Delete a resource.  Works currently with file or s3."
  [url & options]
  (with-provider url
    (io-prot/delete! provider url-parts (args->map options))))


(defn exists?
  "Boolean existence check.  Works with everything as fallback is to open
an input stream and then close it."
  [url & options]
  (with-provider url
    (io-prot/exists? provider url-parts (args->map options))))


(defn metadata
  "If supported, returns at least :modify-date and :byte-length.
Exception otherwise."
  [url & options]
  (with-provider url
    (io-prot/metadata provider url-parts options)))


(defn put-edn!
  "Put edn data to a url"
  [url data & options]
  (with-provider url
    (edn/put-edn! provider url-parts data (args->map options))))


(defn get-edn
  "Get edn data from a url"
  [url & options]
  (with-provider url
    (edn/get-edn provider url-parts (args->map options))))


(defn put-nippy!
  "Put nippy data to a url"
  [url data & options]
  (with-provider url
    (edn/put-nippy! provider url-parts (args->map options))))


(defn get-nippy
  "Get nippy data from a url"
  [url & options]
  (with-provider url
    (edn/get-nippy provider url-parts (args->map options))))


(defn copy
  "Forwards to clojure.java.io/copy"
  [& args]
  (apply io/copy args))
