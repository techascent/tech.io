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


(defn- maybe-cache-stream
  [url-parts options cache-parts cache-options cache-provider src-provider]
  (let [missing? (not (io-prot/exists? cache-provider cache-parts options))
        src-modify (:modify-date (io-prot/metadata src-provider url-parts options))]
    (when (or missing?
              (if (::cache-check-metadata-on-read? cache-options)
                ;;If we can't get a modification date from the src provider then we should only
                ;;get the object if it is missing.
                (and src-modify
                     (date-before? (:modify-date (io-prot/metadata cache-provider cache-parts cache-options))
                                   src-modify))
                true))
      (io-prot/put-object! cache-provider cache-parts
                           (io-prot/get-object src-provider url-parts options)
                           cache-options))))


;;Provider built for static or append-only datasets.  Very limited ability to handle
;;changing datasets.  Also has no full-threshold; will write until cache-provider runs out of space.
(defrecord CacheProvider [url-parts->cache-parts-fn cache-provider src-provider default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          cache-options (merge default-options options)]
      (maybe-cache-stream url-parts options cache-parts cache-options cache-provider src-provider)
      (io-prot/input-stream cache-provider cache-parts options)))
  (output-stream! [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          ^OutputStream cache-output-stream (io-prot/output-stream!
                                             cache-provider
                                             cache-parts
                                             (merge default-options options))]
      (if (::cache-write-through? (merge default-options options))
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

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          cache-options (merge default-options options)]
      (maybe-cache-stream url-parts options cache-parts cache-options cache-provider src-provider)
      (io-prot/get-object cache-provider cache-parts cache-options)))
  (put-object! [provider url-parts value options]
    (when (::cache-write-through? (merge default-options options))
      (io-prot/put-object! src-provider url-parts value options))
    (io-prot/put-object! cache-provider
                         (url-parts->cache-parts-fn url-parts)
                         value
                         (merge default-options options)))

  io-prot/IUrlRedirect
  (url->redirect-url [provider url]
    (-> url
        url/url->parts
        url-parts->cache-parts-fn
        url/parts->url)))


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
    (io-prot/metadata (url-parts->provider url-parts) url-parts (merge default-options options)))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/get-object (url-parts->provider url-parts) url-parts
                        (merge default-options options)))
  (put-object! [provider url-parts value options]
    (io-prot/put-object! (url-parts->provider url-parts) url-parts value
                         (merge default-options options))))

(defn forwarding-provider
  [& {:keys [url-parts->provider]
      :or {url-parts->provider io-prot/url-parts->provider}
      :as options}]
  (->ForwardingProvider url-parts->provider (dissoc options :url-parts->provider)))


(defn url-parts->file-cache
  [cache-dir url-parts]
  (let [target-fname (str "file://" cache-dir
                          "/" (name (:protocol url-parts))
                          "/" (url/string-seq->file-path (:path url-parts)))]
    (url/url->parts target-fname)))


(defn create-file-cache
  [cache-dir {:keys [src-provider
                     ::cache-check-metadata-on-read?
                     ::cache-write-through?]
              :or {cache-check-metadata-on-read? true
                   cache-write-through? true}
              :as cache-options}]
  (let [url-parts->cache-parts (partial url-parts->file-cache cache-dir)]
    (->CacheProvider url-parts->cache-parts
                     (forwarding-provider)
                     (or src-provider (forwarding-provider))
                     (merge cache-options
                            {::cache-check-metadata-on-read? cache-check-metadata-on-read?
                             ::cache-write-through? cache-write-through?}))))
