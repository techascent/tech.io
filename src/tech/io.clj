(ns tech.io
  (:require [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [think.resource.core :as resource])
  (:import [tech.io.protocols IOProvider]
           [java.io File OutputStream
            ByteArrayOutputStream ByteArrayInputStream]))


(defn- io-input-stream
  [url-parts options]
  (io/make-input-stream (url/parts->url url-parts) options))


(defn- io-output-stream
  [url-parts options]
  (io/make-output-stream (url/parts->url url-parts) options))


(extend-protocol io-prot/IOProvider
  Object
  (input-stream [this url-parts options] (io-input-stream url-parts options))
  (output-stream [this url-parts options] (io-output-stream url-parts options))
  (exists? [this url-parts options]
    (try
      (.close (io-prot/input-stream this url-parts options))
      true
      (catch Throwable e false)))
  (ls [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))
  (delete [this url-parts options]
    (throw (ex-info "Unimplemented" url-parts)))

  File
  (input-stream [this url-parts options]
    (io/make-input-stream this options))
  (output-stream [this url-parts options]
    (io/make-parents this)
    (io/make-output-stream this options))
  (exists? [this url-parts options]
    (.exists ^File this))
  (ls [this url-parts options]
    (->> (fs/list-dir this)
         (map (fn [^File f]
                {:url (str "file://" (.toString f))}))))
  (delete [this url-parts options]
    (fs/delete-dir this)))


(defmethod io-prot/url-parts->provider :default
  [& args]
  (Object.))


(defmethod io-prot/url-parts->provider :file
  [url-parts]
  (io/file (url/parts->file-path url-parts)))


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
  (if (url/url? url)
    (with-provider url
      (io-prot/ls provider url-parts (args->map options)))
    (throw (ex-info "Unimplemented" {:url url}))))


(defn delete!
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/delete! provider url-parts (args->map options)))
    (throw (ex-info "Unimplemented" {:url url}))))


(defn exists?
  [url & options]
  (if (url/url? url)
    (with-provider url
      (io-prot/exists? provider url-parts (args->map options)))
    (throw (ex-info "Unimplemented" {:url url}))))


(defn- url-parts->file-cache-url-parts
  [cache-dir-parts url-parts]
  {:protocol :file
   :path (concat cache-dir-parts url-parts)
   :arguments (:arguments url-parts)})


(defn- safe-doall-streams
  "MAke best effort to do this across all streams"
  [op streams]
  (->> streams
       (map #(try (op %)
                  nil
                  (catch Throwable e e)))
       (remove nil?)
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



;;Provider built for static or append-only datasets.  Very limited ability to handle
;;changing datasets.  Also has no full-threshold; will write until cache-provider runs out of space.
(defrecord CacheProvider [url-parts->cache-parts-fn cache-provider src-provider default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (let [cache-parts (url-parts->cache-parts-fn url-parts)
          cache-options (merge default-options options)]
      (when-not (or (io-prot/exists? cache-provider cache-parts options)
                    (if (:cache-check-metadata-on-read? cache-options)
                      (= (:modify-date (io-prot/meta-data cache-provider cache-parts cache-options))
                         (:modify-date (io-prot/meta-data src-provider url-parts options)))
                      true))
        (with-open [in-s (io-prot/input-stream src-provider url-parts options)
                    out-s (io-prot/output-stream! src-provider url-parts (merge default-options options))]
          (io/copy in-s out-s)))
      (io-prot/input-stream cache-provider cache-parts options)))
  (output-stream! [provider url-parts options]
    (let [cache-parts (url-parts->file-cache-url-parts url-parts)
          ^OutputStream cache-output-stream (io-prot/output-stream!
                                             cache-provider
                                             cache-parts
                                             (merge default-options options))]
      (if (:cache-write-through? (merge default-options options))
        (let [src-output-stream (io-prot/output-stream!
                                 src-provider
                                 url-parts
                                 options)]
          (combined-output-streams src-output-stream cache-output-stream))
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
           (url-parts->file-cache-url-parts url-parts)
           (merge default-options options)]]
         (safe-doall-streams (fn [[provider url-parts options]]
                               (io-prot/delete! provider
                                                url-parts
                                                options)))))

  (meta-data [provider url-parts options]
    (or (io-prot/meta-data cache-provider
                           (url-parts->file-cache-url-parts url-parts)
                           (merge default-options options))
        (io-prot/meta-data src-provider url-parts options)))

  io-prot/IUrlCache
  (url->cache-url [provider url-parts options]
    (url-parts->cache-parts-fn url-parts)))


;;Generically forward everything to wherever the url points.
(defrecord ForwardingProvider [url-parts->provider default-options]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (io-prot/input-stream (url-parts->provider url-parts) url-parts options))
  (output-stream! [provider url-parts options]
    (io-prot/output-stream! (url-parts->provider url-parts) url-parts options))
  (exists? [provider url-parts options]
    (io-prot/exists? (url-parts->provider url-parts) url-parts options))
  (ls [provider url-parts options]
    (io-prot/ls (url-parts->provider url-parts) url-parts options))
  (delete! [provider url-parts options]
    (io-prot/delete! (url-parts->provider url-parts) url-parts options))
  (meta-data [provider url-parts options]
    (io-prot/meta-data (url-parts->provider url-parts) url-parts options)))
