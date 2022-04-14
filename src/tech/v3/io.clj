(ns tech.v3.io
  "Wrapper for tech io subsystem designed to be drop in replacement for some use cases
of clojure.java.io."
  (:require [clojure.java.io :as io]
            [tech.v3.io.url :as url]
            [tech.v3.io.protocols :as io-prot]
            [tech.v3.io.edn :as edn]
            [tech.v3.io.base]
            [tech.v3.io.temp-file :as temp-file]
            [tech.v3.resource :as resource]
            [charred.api :as charred])
  (:import [javax.imageio ImageIO]
           [java.io InputStream OutputStream File Writer Reader
            BufferedInputStream]
           [java.awt.image BufferedImage]
           [java.nio.file Files Path StandardCopyOption
            CopyOption]
           [java.util UUID]
           [java.util.zip GZIPOutputStream GZIPInputStream]))


(set! *warn-on-reflection* true)


;;Purists or people using components will want to use the io-protocols directly with
;;providers passed in.  This API is meant to mimic clojure.java.io but in a more
;;extensible way.
(def ^:dynamic ^:no-doc *provider-fn* #(io-prot/url-parts->provider %))


(defmacro ^:no-doc with-provider-fn
  [provider-fn & body]
  `(with-bindings {#'*provider-fn* ~provider-fn }
     ~@body))


(defmacro ^:no-doc with-provider
  [provider & body]
  `(with-provider-fn (constantly ~provider) ~@body))


(defn- args->map
  [args]
  (apply hash-map args))


;;Straight forwards
(def make-parents io/make-parents)
(vary-meta make-parents merge (meta #'io/make-parents))


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


(defn buffered-input-stream
  ^BufferedInputStream [url & options]
  (let [input-stream (apply input-stream url options)]
    (if (instance? BufferedInputStream input-stream)
      input-stream
      (BufferedInputStream. ^InputStream input-stream))))


(defn gzip-input-stream
  ^InputStream [url & options]
  (-> (apply input-stream url options)
      (GZIPInputStream.)))


(defn output-stream!
  "thing->output-stream conversion.  Falls back to clojure.java.io if url is
  not a string url."
  ^OutputStream [url & options]
  (if (url/url? url)
    (lookup-provider url
      (io-prot/output-stream! provider url-parts (args->map options)))
    (apply io/output-stream url options)))


(defn gzip-output-stream!
  ^OutputStream [url & options]
  (-> (apply output-stream! url options)
      (GZIPOutputStream.)))


(defn reader
  "Create a java.io.reader from a thing."
  ^Reader [url & options]
  (if (instance? Reader url)
    url
    (-> (apply input-stream url options)
        io/reader)))


(defn writer!
  "Create a java.io.writer from a thing."
  ^Writer [url & options]
  (if (instance? Writer url)
    url
    (-> (apply output-stream! url options)
        io/writer)))


(defn resource
  [& args]
  (apply io/resource args))


(defn copy
  [src dest & args]
  (with-open [^InputStream in-s (apply input-stream src args)
              ^OutputStream out-s (apply output-stream! dest args)]
    (io/copy in-s out-s)))


(defn ls
  "Return a directory listing.  May be recursive if desired; only works with file
or s3 providers."
  [url & options]
  (if (url/url? url)
    (lookup-provider url
                     (io-prot/ls provider url-parts
                                 (args->map options)))
    (io-prot/ls url {:path [url]} (args->map options))))


(defn delete!
  "Delete a resource.  Works currently with file or s3."
  [url & options]
  (if (url/url? url)
    (lookup-provider
     url
     (io-prot/delete! provider url-parts (args->map options)))
    (io-prot/delete! url {:path [url]} (args->map options))))


(defn exists?
  "Boolean existence check.  Works with everything as fallback is to open
an input stream and then close it."
  [url & options]
  (if (url/url? url)
    (lookup-provider
     url
     (io-prot/exists? provider url-parts (args->map options)))
    (io-prot/exists? url {:path [url]} (args->map options))))


(defn interlocked-copy-to-file
  "Copy first to a temp, then do an atomic move to the destination.  This avoids
  issues with partial files showing up where they shouldn't and a failed io operation
  leading to incomplete results."
  [src dest & options]
  ;;Make sure the temp cannot conflict with anything else.
  (let [temp-fname (str dest (.toString (UUID/randomUUID)))
        dest-file (file dest)
        opt-map (args->map options)
        exist-check! (fn []
                       (when (and (:error-existing? opt-map)
                                  (apply exists? dest options))
                         (throw (ex-info (format "File exists: %s" dest) {}))))]
    (exist-check!)
    (try
      (with-open [^InputStream in-s (apply input-stream src options)
                  ^OutputStream out-s (apply output-stream! temp-fname options)]
        (io/copy in-s out-s))
      (let [^File src-file (file temp-fname)]
        (exist-check!)
        (Files/move (.toPath src-file) (.toPath dest-file)
                    (into-array CopyOption
                                (if (:overwrite-existing? opt-map)
                                  [StandardCopyOption/ATOMIC_MOVE
                                   StandardCopyOption/REPLACE_EXISTING]
                                  [StandardCopyOption/ATOMIC_MOVE]))))
      (finally
        (.delete (java.io.File. temp-fname))))
    dest))


(defn metadata
  "If supported, returns at least :modify-date and :byte-length.
Exception otherwise."
  [url & options]
  (if (url/url? url)
    (lookup-provider url
                     (io-prot/metadata provider url-parts options))
    (io-prot/metadata url {:path [url]} (args->map options))))


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
  (-> (apply output-stream! url options)
      (edn/put-edn! data)))


(defn get-edn
  "Get edn data from a url"
  [url & options]
  (-> (apply input-stream url options)
      edn/get-edn))


(defn put-nippy!
  "Put nippy data to a url"
  [url data & options]
  (-> (apply output-stream! url options)
      (edn/put-nippy! data)))


(defn get-nippy
  "Get nippy data from a url"
  [url & options]
  (-> (apply input-stream url options)
      edn/get-nippy))


(defn put-image!
  "Will throw if an image with transparency is used to write a jpeg"
  [url image & options]
  (let [opt-map (args->map options)
        ^String ext (or (:extension opt-map)
                        (url/extension url))]
    (when-not (or (string? url)
                  (:extension opt-map))
      (throw (ex-info "Image type must be specified by either the extension
of the url or explicity via an ':extension' optional argument"
                      {})))
    (with-open [^OutputStream out-s (apply output-stream! url options)]
      (ImageIO/write ^BufferedImage image
                     ext
                     out-s))))


(defn get-image
  [url & options]
  (with-open [^InputStream in-s (apply input-stream url options)]
    (ImageIO/read ^InputStream in-s)))


(defn put-json!
  "Write json.  Options are used both for constructing the output stream
  and passed into the json write method.  See documentation for
  clojure.data.json/write."
  [url data & options]
  (with-open [^Writer writer (apply writer! url options)]
    (apply charred/write-json writer data options)))


(defn get-json
  "Read json.  Options are used both for constructing input stream
  and passed into the json read method.  See documentation for
  clojure.data.json/read"
  [url & options]
  (with-open [^Reader reader (apply reader url options)]
    (apply charred/read-json reader options)))


(defn default-csv-key-printer
  [item-key]
  (if (or (symbol? item-key)
          (keyword? item-key))
    (name item-key)
    (str item-key)))


(defn mapseq->csv!
  "Given a sequence of maps, produce a csv or tsv.  Options are passed
  via apply to clojure.data.csv/write-csv.
  Valid options for this method are:
  :key-printer - default print method to use else default-csv-key-printer
  :key-seq - ordered sequence of keys to pull out of the maps.  Defaults
    to sorted order of printed keys of the first map.
  :separator - Which separator to use, defaults to ','"
  [url map-seq & options]
  (let [opt-map (args->map options)
        key-fn (or (:key-printer opt-map)
                   default-csv-key-printer)
        data-keys
        (if-let [user-keys (:key-seq opt-map)]
          (->> user-keys
               (map (juxt identity key-fn)))
          (->> (keys (first map-seq))
               (map (juxt identity key-fn))
               (sort-by second)))
        map-keys (mapv first data-keys)
        column-names (mapv second data-keys)]
    (with-open [^Writer writer (apply writer! url options)]
      (apply charred/write-csv
             writer
             (concat [column-names]
                     (->> map-seq
                          (map (fn [entry]
                                 (->> map-keys
                                      (mapv #(get entry %)))))))
             options))))


(defn- csv-sequence->mapseq
  [opt-map csv-seq]
  (let [key-fn (or (:key-fn opt-map)
                   keyword)
        map-keys (->> (first csv-seq)
                      (mapv (or key-fn identity)))
        csv-seq (rest csv-seq)]
    (->> csv-seq
         (map (fn [next-values]
                (into {} (map vector map-keys next-values)))))))


(defn autodetect-csv-separator
  "Scan first 100 characters, count commas and tabs.  Whichever one
  wins is the separator."
  [url & options]
  (let [opt-map (args->map options)
        n-chars (or (:n-chars opt-map)
                    100)
        possible-separators (or (:separators opt-map)
                                [\, \tab])
        char-data (char-array n-chars)
        detection-fn
        (fn [^Reader reader]
          (let [num-read (.read reader char-data)
                char-table (frequencies char-data)
                n-tabs (get char-table \tab 0)
                n-commas (get char-table \, 0)]
            (->> possible-separators
                 (reduce (fn [max-data next-sep]
                           (let [n-chars (get char-table next-sep 0)]
                             (if (or (not max-data)
                                     (> n-chars (first max-data)))
                               [n-chars next-sep]
                               max-data)))
                         nil)
                 second)))]
    (if (instance? Reader url)
      (detection-fn url)
      (with-open [^Reader reader (apply reader url options)]
        (detection-fn reader)))))


(defn csv->mapseq
  "Given a csv, produce a sequence of maps.  This is mainly to be used for
  specific use cases like processing the data.  For large datasets that you
  intend to do pandas-style dataset processing, please see tech.ml.dataset.
  If the input is not a reader, the sequence is completely read so the input stream
  can be closed.  If the input is a reader, a lazy sequence of maps is returned.

  The delimiter is auto-detected by scanning the first 100 or so characters.
  :separator - hard set csv separator to use.
  :skip-autodetect - Don't autodetect, use ',' as separator."
  [url & options]
  (let [opt-map (args->map options)
        detect-fn (fn [^Reader reader]
                    (let [separator
                          (cond
                            (:separator options)
                            (:separator options)
                            (or (:skip-autodetect options)
                                (not (.markSupported reader)))
                            \,
                            :else
                            (do
                              (.mark reader 200)
                              (let [separator
                                    (apply autodetect-csv-separator reader options)]
                                (.reset reader)
                                separator)))]
                      (->> (assoc opt-map :separator separator)
                           (apply concat))))]
    (if (or (instance? Reader url)
            (instance? InputStream url))
      (let [reader (reader url)]
        (->> (apply charred/read-csv reader (detect-fn reader))
             (csv-sequence->mapseq opt-map)))
      (with-open [^Reader reader (apply reader url options)]
        (->> (apply charred/read-csv reader (detect-fn reader))
             (csv-sequence->mapseq opt-map)
             doall)))))
