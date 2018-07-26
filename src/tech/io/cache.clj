(ns tech.io.cache
  "Small caching layer to handle persistent, ever growing file caching"
  (:require [clojure.java.io :as io]
            [tech.io.protocols :as io-prot]
            [tech.io.url :as url])
  (:import [java.io InputStream OutputStream]
           [java.util Date]))


(defn- safe-doall-streams
  "Make best effort to do this across all streams"
  [op streams]
  (->> streams
       (map #(try (op %)
                  nil
                  (catch Throwable e e)))
       (remove nil?)
       ;;Force everything here
       vec
       ;;throw at first error here
       (map #(throw %))
       dorun))


(defn- combined-output-streams
  ^OutputStream [streams]
  (proxy [OutputStream] []
    (close []
      (safe-doall-streams #(.close ^OutputStream %) streams))
    (flush []
      (safe-doall-streams #(.flush ^OutputStream %) streams))
    (write
      ([b]
       (if (bytes? b)
         (safe-doall-streams #(.write ^OutputStream % ^bytes b) streams)
         (safe-doall-streams #(.write ^OutputStream % ^int b) streams)))
      ([b off len]
       (safe-doall-streams #(.write ^OutputStream % b off len) streams)))))


(defn- date-before?
  [^Date d1 ^Date d2]
  (< (.getTime d1)
     (.getTime d2)))


;;Provider built for static or append-only datasets.  Very limited ability to handle
;;changing datasets.  Also has no full-threshold; will write until cache-provider runs out of space.
(defrecord CacheProvider [url-parts->cache-parts-fn cache-provider src-provider default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          cache-options (merge default-options options)
          missing? (not (io-prot/exists? cache-provider cache-parts options))]
      (when (or missing?
                (if (:cache-check-metadata-on-read? cache-options)
                  (date-before? (:modify-date (io-prot/metadata cache-provider cache-parts cache-options))
                                (:modify-date (io-prot/metadata src-provider url-parts options)))
                  true))
        (with-open [in-s (io-prot/input-stream src-provider url-parts options)
                    out-s (io-prot/output-stream! cache-provider cache-parts
                                                  (merge default-options options
                                                         {:metadata (io-prot/metadata
                                                                     src-provider
                                                                     url-parts
                                                                     options)}))]
          (io/copy in-s out-s)))
      (io-prot/input-stream cache-provider cache-parts options)))
  (output-stream! [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          ^OutputStream cache-output-stream (io-prot/output-stream!
                                             cache-provider
                                             cache-parts
                                             (merge default-options options))]
      (if (:cache-write-through? (merge default-options options))
        (let [src-output-stream (io-prot/output-stream!
                                 src-provider
                                 url-parts
                                 options)]
          (combined-output-streams [src-output-stream cache-output-stream]))
        cache-output-stream)))

  (exists? [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          cache-options (merge default-options options)]
      (or (io-prot/exists? cache-provider
                           cache-parts
                           cache-options)
          (io-prot/exists? src-provider
                           url-parts
                           options))))
  (ls [provider url-parts options]
    (io-prot/ls src-provider
                url-parts
                options))

  (delete! [provider url-parts options]
    (->> [[src-provider url-parts options]
          [cache-provider
           (url-parts->cache-parts-fn url-parts)
           (merge default-options options)]]
         (safe-doall-streams (fn [[provider url-parts options]]
                               (io-prot/delete! provider
                                                url-parts
                                                options)))))

  (metadata [provider url-parts options]
    (or (io-prot/metadata cache-provider
                           (url-parts->cache-parts-fn url-parts)
                           (merge default-options options))
        (io-prot/metadata src-provider url-parts options)))

  io-prot/IUrlCache
  (url->cache-url [provider url-parts options]
    (url-parts->cache-parts-fn url-parts)))


;;Generically forward everything to wherever the url points.
(defrecord ForwardingProvider [url-parts->provider default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (io-prot/input-stream (url-parts->provider url-parts) url-parts (merge default-options options)))
  (output-stream! [provider url-parts options]
    (io-prot/output-stream! (url-parts->provider url-parts) url-parts (merge default-options options)))
  (exists? [provider url-parts options]
    (io-prot/exists? (url-parts->provider url-parts) url-parts (merge default-options options)))
  (ls [provider url-parts options]
    (io-prot/ls (url-parts->provider url-parts) url-parts (merge default-options options)))
  (delete! [provider url-parts options]
    (io-prot/delete! (url-parts->provider url-parts) url-parts (merge default-options options)))
  (metadata [provider url-parts options]
    (io-prot/metadata (url-parts->provider url-parts) url-parts (merge default-options options))))


(defn url-parts->file-cache
  [cache-dir url-parts]
  (let [target-fname (str "file://" cache-dir
                          "/" (name (:protocol url-parts))
                          "/" (url/string-seq->file-path (:path url-parts)))]
    (url/url->parts target-fname)))


(defn create-file-cache
  [cache-dir {:keys [src-provider
                     cache-check-metadata-on-read?
                     cache-write-through?]
              :or {cache-check-metadata-on-read? true
                   cache-write-through? true}
              :as cache-options}]
  (let [url-parts->cache-parts (partial url-parts->file-cache cache-dir)]
    (->CacheProvider url-parts->cache-parts
                     (->ForwardingProvider io-prot/url-parts->provider {})
                     (or src-provider (->ForwardingProvider io-prot/url-parts->provider {}))
                     (merge cache-options
                            {:cache-check-metadata-on-read? cache-check-metadata-on-read?
                             :cache-write-through? cache-write-through?}))))
