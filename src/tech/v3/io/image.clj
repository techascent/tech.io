(ns tech.v3.io.image
  (:require [tech.v3.io.common :refer [args->map]]
            [tech.v3.io.url :as url])
  (:import [javax.imageio ImageIO]
           [java.awt.image BufferedImage]
           [java.io InputStream OutputStream]))

(set! *warn-on-reflection* true)

(defn put-image!
  "Will throw if an image with transparency is used to write a jpeg"
  [url image output-stream! options]
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
  [url input-stream options]
  (with-open [^InputStream in-s (apply input-stream url options)]
    (ImageIO/read ^InputStream in-s)))
