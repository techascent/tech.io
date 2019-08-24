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
  [^OutputStream output-stream data]
  (with-open [out-s output-stream]
    (.write out-s (edn->bytes data))))


(defn get-edn
  [input-stream]
  (-> (input-stream->bytes input-stream)
      (String.)
      (edn/read-string)))


(defn put-nippy!
  [output-stream data]
  (with-open [^OutputStream out-s output-stream]
    ;;Writing frozen data is faster and leads to higher compression ratios than using
    ;;the nippy stream operators
    (.write out-s ^bytes (nippy/freeze data))))


(defn get-nippy
  [input-stream]
  (with-open [^InputStream in-s input-stream
              byte-s (ByteArrayOutputStream.)]
    (io/copy in-s byte-s)
    (nippy/thaw (.toByteArray byte-s))))
