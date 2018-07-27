(ns tech.io.redirect
  "Provider that redirects one protocol to another preserving path"
  (:require [tech.io.protocols :as io-prot]
            [tech.io.cache :as io-cache]))

(defrecord RedirectProvider [url-parts->url-parts-fn src-provider default-options]
    io-prot/IOProvider
  (input-stream [provider url-parts options]
    (io-prot/input-stream src-provider (url-parts->url-parts-fn url-parts)
                          (merge default-options options)))
  (output-stream! [provider url-parts options]
    (io-prot/output-stream! src-provider (url-parts->url-parts-fn url-parts)
                            (merge default-options options)))
  (exists? [provider url-parts options]
    (io-prot/exists? src-provider (url-parts->url-parts-fn url-parts)
                     (merge default-options options)))
  (ls [provider url-parts options]
    (io-prot/ls src-provider (url-parts->url-parts-fn url-parts)
                (merge default-options options)))
  (delete! [provider url-parts options]
    (io-prot/delete! src-provider (url-parts->url-parts-fn url-parts)
                     (merge default-options options)))
  (metadata [provider url-parts options]
    (io-prot/metadata src-provider (url-parts->url-parts-fn url-parts)
                      (merge default-options options)))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/get-object src-provider (url-parts->url-parts-fn url-parts)
                        (merge default-options options)))
  (put-object! [provider url-parts value options]
    (io-prot/put-object! src-provider (url-parts->url-parts-fn url-parts)
                         value (merge default-options options))))


(defn create-file-provider
  "Redirect all protocols to access file system.  Useful if you want to, for instance,
mock up s3 but only using local filesystem."
  [cache-dir {:keys [src-provider]
              :as redirect-options}]
  (->RedirectProvider (partial io-cache/url-parts->file-cache cache-dir)
                      (or src-provider
                          (io-cache/forwarding-provider))
                      redirect-options))
