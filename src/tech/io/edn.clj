(ns tech.io.edn
  "Get/put edn data to/from any valid io provider"
  (:require [taoensso.nippy :as nippy]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [tech.io.protocols :as io-prot])
  (:import [java.nio.charset Charset]
           [java.io OutputStream InputStream ByteArrayOutputStream]))


(set! *warn-on-reflection* true)


(defn edn->str
  ^String [edn-data]
  ;;many bothans died to bring us this information
  (binding [*print-length* false]
    (pr-str edn-data)))


(defn edn->bytes
  ^bytes [edn-data]
  (-> (edn->str edn-data)
      (.getBytes (Charset/forName "UTF-8"))))


(defn input-stream->bytes
  "Closes input stream"
  ^bytes [^InputStream in-s]
  (with-open [in-s in-s
              ^OutputStream byte-s (ByteArrayOutputStream.)]
    (io/copy in-s byte-s)
    (.toByteArray ^ByteArrayOutputStream byte-s)))


(defn put-edn!
  [provider url-parts data options]
  (with-open [^OutputStream out-s (io-prot/output-stream! provider url-parts options)]
    (.write out-s (edn->bytes data))))


(defn get-edn
  [provider url-parts options]
  (-> (io-prot/input-stream provider url-parts options)
      input-stream->bytes
      (String.)
      (edn/read-string)))


(defn put-nippy!
  [provider url-parts data options]
  (with-open [^OutputStream out-s (io-prot/output-stream! provider url-parts options)]
    ;;Writing frozen data is faster and leads to higher compression ratios than using
    ;;the nippy stream operators
    (.write out-s ^bytes (nippy/freeze data))))


(defn get-nippy
  [provider url-parts options]
  (with-open [^InputStream in-s (io-prot/input-stream provider url-parts options)
              byte-s (ByteArrayOutputStream.)]
    (io/copy in-s byte-s)
    (nippy/thaw (.toByteArray byte-s))))
