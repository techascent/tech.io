(ns tech.io
  (:require [clojure.java.io :as io]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [tech.io.edn :as edn]
            [tech.io.base]))

;;Purists or poeple using components will want to use the io-protocols directly with providers passed in.
;;This API is meant to mimic clojure.java.io but in a more extensible way.

(def ^:dynamic *provider* nil)

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
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/input-stream provider url-parts (args->map options)))
    (apply io/input-stream url options)))



(defn output-stream!
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/output-stream! provider url-parts (args->map options)))
    (apply io/output-stream url options)))


(defn ls
  [url & options]
  (with-provider url
    (io-prot/ls provider url-parts (args->map options))))


(defn delete!
  [url & options]
  (with-provider url
    (io-prot/delete! provider url-parts (args->map options))))


(defn exists?
  [url & options]
  (with-provider url
    (io-prot/exists? provider url-parts (args->map options))))


(defn metadata
  [url & options]
  (with-provider url
    (io-prot/metadata provider url-parts options)))


(defn put-edn
  [url data & options]
  (with-provider url
    (edn/put-edn provider url-parts data (args->map options))))


(defn get-edn
  [url & options]
  (with-provider url
    (edn/get-edn provider url-parts (args->map options))))


(defn put-nippy
  [url data & options]
  (with-provider url
    (edn/put-nippy provider url-parts (args->map options))))


(defn get-nippy
  [url & options]
  (with-provider url
    (edn/put-nippy provider url-parts (args->map options))))