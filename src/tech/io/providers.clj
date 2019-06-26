(ns tech.io.providers
  (:require [tech.config.core :as config]
            [tech.io.protocols :as io-prot]
            [tech.io.base :as base]
            [tech.parallel.require :as parallel-req]))


(defn caching-provider
  [cache-dir options]
  ((parallel-req/require-resolve 'tech.io.cache/create-file-cache)
   (or cache-dir (config/get-config :tech-io-cache-dir))
   options))


(defn redirect-provider
  [redirect-dir options]
  ((parallel-req/require-resolve 'tech.io.redirect/create-file-provider)
   (or redirect-dir (config/get-config :tech-io-redirect-dir))
   options))


(def ^:dynamic ^:private *default-vault-auth-providers*
  (atom {}))


(defn wrap-provider
  "Works like middlewear.  But the providers have to implement it themselves; most
general purpose (cache, redirect, auth) implement it via their src-provider member."
  ([prov-inner prov-outer]
   (assoc prov-outer
          :src-provider prov-inner))
  ([] nil))


(defn provider-seq->wrapped-providers
  "Given a sequence of providers, wrap them such that the first provider is the outer
  provider.  This means that data will travel through the sequence in a left-to-right or
  top-to-bottom order.  Returns the outer provider or nil of seq is empty"
  [provider-seq]
  (->> provider-seq
       reverse
       (reduce wrap-provider)))


(def default-provider
  (memoize
   (fn []
     (->> [[:tech-io-cache-local #(caching-provider nil {})]
           [:tech-io-redirect-local #(redirect-provider nil {})]]
          (map (fn [[config-key provider-fn]]
                 (when (config/get-config config-key)
                   (provider-fn))))
          (remove nil?)
          provider-seq->wrapped-providers))))


(defmethod io-prot/url-parts->provider :default
  [& args]
  (Object.))


(defmethod io-prot/url-parts->provider :file
  [url-parts]
  (base/parts->file url-parts))
