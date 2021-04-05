(ns tech.v3.io.amperity-vault
  (:require [tech.config.core :as config]
            [vault.core :as vault]
            [vault.client.http]))


(def client (-> (vault/new-client (config/get-config :vault-addr))
                (vault/authenticate! :token
                                     (slurp (str (System/getProperty "user.home")
                                                 "/.vault-token")))))

(defn read-credentials
  [cred-path]
  (vault/read-secret client cred-path))
