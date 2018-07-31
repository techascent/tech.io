(ns tech.io.providers
  (:require [tech.config.core :as config]
            [com.stuartsierra.component :as c]))


(defn caching-provider
  [cache-dir options]
  (require 'tech.io.cache)
  ((resolve 'tech.io.cache/create-file-cache)
   (or cache-dir (config/get-config :tech-io-cache-dir))
   options))


(defn redirect-provider
  [redirect-dir options]
  (require 'tech.io.redirect)
  ((resolve 'tech.io.redirect/create-file-provider)
   (or redirect-dir (config/get-config :tech-io-redirect-dir))
   options))


(def ^:dynamic ^:private *default-vault-auth-providers*
  (atom {}))


(defn vault-auth-provider
  [vault-path options]
  (require 'tech.io.auth)
  (let [map-key [vault-path options]
        providers @*default-vault-auth-providers*]
    (if-let [provider (get providers map-key)]
      provider
      (do
        ;;only allow this once, nil->provider only valid transition
        (compare-and-set! *default-vault-auth-providers*
                          providers
                          (assoc providers map-key
                                 ((resolve 'tech.io.auth/vault-aws-auth-provider)
                                  (or vault-path (config/get-config :tech-io-vault-cred-path))
                                  options)))
        ;;start is idempotent
        (swap! *default-vault-auth-providers* update map-key c/start)
        (recur vault-path options)))))


(defn shutdown-providers!
  []
  (let [providers @*default-vault-auth-providers*]
    (compare-and-set! *default-vault-auth-providers* providers {})
    (->> providers
         (map (comp c/stop second))
         dorun)
    :ok))


(defn wrap-provider
  "Works like middlewear.  But the providers have to implement it themselves; most
general purpose (cache, redirect, auth) implement it via their src-provider member."
  ([prov-outer prov-inner]
   (assoc prov-outer
          :src-provider prov-inner))
  ([] nil))


(def default-provider
  (memoize
   (fn []
     (->> [[:tech-io-vault-auth #(vault-auth-provider nil {})]
           [:tech-io-cache-local #(caching-provider nil {})]
           [:tech-io-redirect-local #(redirect-provider nil {})]]
          (map (fn [[config-key provider-fn]]
                 (when (config/get-config config-key)
                   (provider-fn))))
          (remove nil?)
          (reduce wrap-provider)))))
