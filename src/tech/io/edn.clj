(ns tech.io.edn
  "Get/put edn data to/from any valid io provider"
  (:require [taoensso.nippy :as nippy]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [tech.io.protocols :as io-prot])
  (:import [java.nio.charset Charset]
           [java.io ByteArrayOutputStream]))


(defn edn->str
  [edn-data]
  ;;many bothans died to bring us this information
  (binding [*print-length* false]
    (pr-str edn-data)))


(defn edn->bytes
  [edn-data]
  (-> (edn->str edn-data)
      (.getBytes (Charset/forName "UTF-8"))))


(defn input-stream->bytes
  "Closes input stream"
  [in-s]
  (with-open [in-s in-s
              byte-s (ByteArrayOutputStream.)]
    (io/copy in-s byte-s)
    (.toByteArray byte-s)))


(defn put-edn!
  [provider url-parts data options]
  (with-open [out-s (io-prot/output-stream! provider url-parts options)]
    (.write out-s (edn->bytes data))))


(defn get-edn
  [provider url-parts options]
  (-> (io-prot/input-stream provider url-parts options)
      input-stream->bytes
      (String.)
      (edn/read-string)))


(defn put-nippy!
  [provider url-parts data options]
  (with-open [out-s (io-prot/output-stream! provider url-parts options)]
    ;;Writing frozen data is faster and leads to higher compression ratios than using
    ;;the nippy stream operators
    (.write out-s (nippy/freeze data))))


(defn get-nippy
  [provider url-parts options]
  (with-open [in-s (io-prot/input-stream provider url-parts options)
              byte-s (ByteArrayOutputStream.)]
    (io/copy in-s byte-s)
    (nippy/thaw (.toByteArray byte-s))))
