(ns tech.io.s3
  "Access to s3 resources via the io provider abstraction"
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3transfer]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [taoensso.timbre :as log]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [tech.io.base :as base])
  (:import [com.amazonaws.services.s3.model AmazonS3Exception]
           [java.nio.file Files Path FileSystems Paths]
           [java.io File ByteArrayOutputStream OutputStream]
           [java.util Base64]
           [java.security MessageDigest]
           [org.joda.time DateTime]))

(defn is-invalid-access-key?
  [exception]
  (and (instance? AmazonS3Exception exception)
       (= (.getErrorCode exception) "InvalidAccessKeyId")))

(defn is-null-access-key?
  [exception]
  (and (instance? IllegalArgumentException exception)
       (= (.getMessage exception) "Access key cannot be null.")))

(defn is-expired-token?
  [exception]
  (and (instance? AmazonS3Exception exception)
       (= (.getErrorCode exception) "ExpiredToken")))


(defn- call-s3-fn
  [s3-fn arg-map {:keys [:tech.aws/access-key
                         :tech.aws/secret-key
                         :tech.aws/session-token
                         :tech.aws/endpoint]
                  :as options}]
  (let [cred-map (cond-> {}
                   (and access-key secret-key)
                   (assoc :access-key access-key :secret-key secret-key)
                   (and access-key secret-key session-token)
                   (assoc :session-token session-token)
                   endpoint
                   (assoc :endpoint endpoint))
        flattened-args (-> arg-map
                           seq
                           flatten)]
    (if (> (count cred-map) 0)
      (apply s3-fn cred-map flattened-args)
      (apply s3-fn flattened-args))))


(defn get-object
  [bucket k options]
  (let [retval (call-s3-fn s3/get-object {:bucket-name bucket
                                          :key k}
                           options)]
    (get retval :input-stream)))


(defn get-content-type
  [fname]
  (Files/probeContentType (Paths/get fname (into-array String []))))


(defn- is-byte-array?
  [v]
  (instance? (Class/forName "[B") v))

(def empty-file-md5 "1B2M2Y8AsgTpgAmY7PhCfg==")

(defn byte-array-to-md5
  ^bytes [^bytes byte-data]
  (let [digest (MessageDigest/getInstance "MD5")]
    (.digest digest byte-data)))


(defn- base-64-encode
  ^String [^bytes byte-data]
  (String. (.encode (Base64/getEncoder) byte-data)))


(defn put-object!
  "Lots of smarts around put-object to ensure the entire object is written correctly
or the write fails.  We check md5 hash if possible and we attempt to set the content type
in order to ensure that if, for instance, you write a jpeg to an open bucket that jpeg
is accessible via the browser using a normal https request."
  [bucket k v {:keys [::metadata ::verify-md5?]
               :or {metadata {}}
               :as options}]
  (let [[metadata v] (if (or verify-md5? (is-byte-array? v))
                       (let [byte-data (if-not (is-byte-array? v)
                                         (let [temp-stream (ByteArrayOutputStream.)]
                                           (io/copy v temp-stream)
                                           (.close temp-stream)
                                           (.toByteArray temp-stream))
                                         v)
                             md5-str (base-64-encode (byte-array-to-md5 byte-data))]
                         [(merge {:content-length (count byte-data)
                                  :content-md5 md5-str})
                          byte-data])
                       [metadata v])
        content-type (get-content-type k)
        content-length (cond (is-byte-array? v) (count v)
                             (instance? File v) (.length v)
                             :else nil)
        metadata (merge (if content-type {:content-type content-type})
                        (if content-length {:content-length content-length})
                        metadata)]
    ;;If you do not set the content type in the metadata then for instance images will be
    ;;inaccessible via http request
    (when (and content-length (zero? content-length))
      (throw (ex-info "Zero length content detected" {:bucket bucket
                                                      :key k})))
    (let [result (call-s3-fn s3/put-object {:bucket-name bucket
                                            :key k
                                            :input-stream (io/input-stream v)
                                            :metadata metadata}
                             options)
          content-hash (get result :content-md5)]
      (when (= content-hash empty-file-md5)
        (throw (Exception. (format "Zero byte write detected: %s %s" bucket k))))
      result)))


(defn delete-object!
  [bucket k options]
  (call-s3-fn s3/delete-object {:bucket-name bucket
                                :key k}
              options))


(defn list-some-objects
  "List objects given a bucket and prefix"
  [bucket prefix {:keys [::marker ::delimiter]
                  :or {delimiter "/"}
                  :as options}]
  (let [retval
        (call-s3-fn s3/list-objects-v2  (merge
                                         {:prefix prefix
                                          :bucket-name bucket
                                          :marker marker}
                                         (when delimiter
                                           {:delimiter delimiter}))
                    options)]
    retval))


(defn lazy-object-list-seq
  [bucket prefix options]
  (let [list-result (list-some-objects bucket prefix options)]
    (if (:next-marker list-result)
      (cons list-result (lazy-seq
                         (lazy-object-list-seq
                          bucket prefix (assoc options ::marker (:next-marker list-result)))))
      (list list-result))))


(defn list-objects
  [bucket prefix {:keys [::delimiter]
                  :or {delimiter "/"}
                  :as options}]
  (let [options (if delimiter
                  (assoc options ::delimiter delimiter)
                  options)
        retval-seq (lazy-object-list-seq bucket prefix options)]
    (concat (->> (:common-prefixes (first retval-seq))
                 (map (fn [prefix]
                        {:bucket-name bucket
                         :key prefix
                         :directory? true})))
            (mapcat :object-summaries retval-seq))))


(defn get-object-metadata
  [bucket key options]
  (call-s3-fn s3/get-object-metadata
              {:bucket-name bucket
               :key key}
              options))


(defn url-parts->bucket
  [{:keys [path]}]
  (first path))


(defn url-parts->key
  [{:keys [path]}]
  (s/join "/" (rest path)))


(defrecord S3Provider [default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (get-object (url-parts->bucket url-parts)
                (url-parts->key url-parts)
                (merge default-options options)))

  (output-stream! [provider url-parts options]
    ;;Write the output stream on close
    (let [byte-stream (ByteArrayOutputStream.)]
      (proxy [OutputStream] []
          (close
            []
            (let [byte-data (.toByteArray byte-stream)]
              (io-prot/put-object! provider url-parts byte-data (merge default-options options))))
        (flush
          [])
        (write
          ([b]
           (if (bytes? b)
             (.write byte-stream ^bytes b)
             (.write byte-stream (int b))))
          ([b off len]
           (.write byte-stream b off len))))))
  (exists? [provider url-parts options]
    (try
      (io-prot/metadata provider url-parts (merge default-options options))
      true
      (catch Throwable e
        false)))
  (ls [provider url-parts {:keys [::delimiter recursive?]
                           :or {delimiter "/"
                                recursive? false}
                           :as options}]
    (let [bucket (url-parts->bucket url-parts)
          key (url-parts->key url-parts)
          ;;If you want a recursive search then you pass in recursive? options
          options (if (:recursive? options)
                    (merge default-options options)
                    (merge default-options (assoc options ::delimiter delimiter)))
          key (if (and delimiter
                       (not (.endsWith key delimiter))
                       (> (count key) 0))
                (str key delimiter)
                key)]
      (->> (list-objects (url-parts->bucket url-parts)
                         key
                         (merge default-options options))
           (map (fn [{:keys [key size directory?]}]
                  (merge
                   {:url (str "s3://" bucket "/" key )
                    :directory? (boolean directory?)}
                   (when size
                     {:byte-length size})))))))
  (delete! [provider url-parts options]
    (delete-object! (url-parts->bucket url-parts)
                    (url-parts->key url-parts)
                    (merge default-options options)))
  (metadata [provider url-parts options]
    (let [{:keys [content-length last-modified content-type]}
          (get-object-metadata (url-parts->bucket url-parts)
                               (url-parts->key url-parts)
                               (merge default-options options))]
      (merge
       {:byte-length (long content-length)
        :modify-date (.toDate ^DateTime last-modified)}
       (when content-type
         {:content-type content-type}))))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/input-stream provider url-parts options))

  (put-object! [provider url-parts value options]
    (put-object! (url-parts->bucket url-parts)
                 (url-parts->key url-parts)
                 value
                 (merge default-options options))
    (let [log-level (::log-level options)
          item-length (cond
                        (bytes? value)
                        (alength ^bytes value)
                        (instance? File value)
                        (:byte-length (base/file->byte-length value))
                        :else
                        nil)]
      (when log-level
        (if item-length
          (log/log log-level (format "s3 write: %s: %s bytes"
                                     (url/parts->url url-parts)
                                     item-length))
          (log/log log-level (format "s3 write: %s" (url/parts->url url-parts))))))))


(defn s3-provider
  [default-options]
  (->S3Provider default-options))


(defn s3-bucket-key->url
  [bucket key]
  (str "s3://" bucket "/" key))


(defn s3-bucket-and-key->https-url
  [s3-bucket s3-key & {:keys [::endpoint]
                       :or {endpoint "us-west-2"}
                       :as options}]
  (format "https://s3-%s.amazonaws.com/%s/%s" endpoint s3-bucket s3-key))


(defn s3-url->https-url
  [s3-url & {:keys [::endpoint]
             :or {endpoint "us-west-2"}
             :as options}]
  (let [{:keys [protocol path] :as url-parts} (url/url->parts s3-url)
        bucket (url-parts->bucket url-parts)
        key (url-parts->key url-parts)]
    (when-not (= :s3 protocol)
      (throw (ex-info "Url is not an s3 url" url-parts)))
    (s3-bucket-and-key->https-url bucket key ::endpoint endpoint)))
