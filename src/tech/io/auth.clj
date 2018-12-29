(ns tech.io.auth
  "Authentication layer designed to work with hashicorp vault's aws credentialling
  system.  Given a function that takes no arguments but can produce a credential map, we
  want to store the latest version of the map but also be prepared for the current
  credentials to time out thus necessitating a new auth request.  In order to do this,
  providers need to throw exceptions of the type:
  (ex-info \"Doesn't matter\" {:exception-action :request-credentials}
  This layer will then catch such exceptions and attempt threadsafe reauthentication."
  (:require [tech.io.protocols :as io-prot]
            [taoensso.timbre :as log]
            [tech.io.cache :as cache]
            [tech.io.url :as url]
            [tech.parallel :as parallel]))


(defn get-credentials
  [re-request-ms src-cred-fn credential-atom]
  (locking credential-atom
    (-> (swap! credential-atom
               (fn [{:keys [access-time] :as cred-data}]
                 (let [cur-time (System/currentTimeMillis)]
                   (if (or (not access-time)
                           (> (- cur-time
                                 (long access-time))
                              (long re-request-ms)))
                     (let [new-creds (src-cred-fn)]
                       {:access-time (System/currentTimeMillis)
                        :credentials new-creds})
                     cred-data))))
        :credentials)))


(defrecord AuthProvider [request-credentials-fn
                         credential-atom
                         src-provider]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (io-prot/input-stream src-provider url-parts (merge (request-credentials-fn)
                                                        options)))
  (output-stream! [provider url-parts options]
    (io-prot/output-stream! src-provider url-parts (merge (request-credentials-fn)
                                                          options)))
  (exists? [provider url-parts options]
    (io-prot/exists? src-provider url-parts (merge (request-credentials-fn)
                                                   options)))
  (ls [provider url-parts options]
    (io-prot/ls src-provider url-parts (merge (request-credentials-fn)
                                              options)))
  (delete! [provider url-parts options]
    (io-prot/delete! src-provider url-parts (merge (request-credentials-fn)
                                                   options)))
  (metadata [provider url-parts options]
    (io-prot/metadata src-provider url-parts (merge (request-credentials-fn)
                                                    options)))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/get-object src-provider url-parts (merge (request-credentials-fn)
                                                      options)))
  (put-object! [provider url-parts value options]
    (io-prot/put-object! src-provider url-parts value (merge (request-credentials-fn)
                                                             options))))


(defn auth-provider
  "You need to call com.stuartsierra.component/start on this to enable the credential
  request system."
  [cred-fn {:keys [
                   ;; How long to wait for a credential request before signalling error.
                   re-request-time-ms
                   src-provider]
            :or {
                 ;;Save credentials for 20 minutes
                 re-request-time-ms (* 20 60 1000)
                 src-provider (cache/forwarding-provider :url-parts->provider
                                                         io-prot/url-parts->provider)}}]
  (let [cred-atom (atom {})
        request-cred-fn #(get-credentials re-request-time-ms cred-fn cred-atom)]
    (->AuthProvider request-cred-fn cred-atom src-provider)))


(defn get-vault-aws-creds
  [vault-path options]
  (log/debug (format "Request vault information: %s" vault-path))
  (let [vault-request ((parallel/require-resolve 'tech.vault-clj.core/read-credentials)
                       vault-path)
        data (get vault-request "data")
        vault-errors (get vault-request "errors")]
    (when-not data
      (throw (ex-info "Failed to access vault information:"
                      (merge {:vault-path vault-path
                              :vault-errors vault-errors}))))
    (merge {:tech.aws/access-key (or (get data "access_key")
                                     (get data "AWS_ACCESS_KEY_ID"))
            :tech.aws/secret-key (or (get data "secret_key")
                                     (get data "AWS_SECRET_ACCESS_KEY"))}
           (when-let [token (or (get data "security_token")
                                (get data "AWS_SESSION_TOKEN"))]
             {:tech.aws/session-token token}))))


(defn vault-aws-auth-provider
  [vault-path options]
  (auth-provider #(get-vault-aws-creds vault-path options) options))



(comment

  (def test-provider (vault-aws-auth-provider "bad/vault/path"
                                              {:re-request-time-ms 1000}))

  ((:request-credentials-fn test-provider))

  (def test-provider (vault-aws-auth-provider (tech.config.core/get-config
                                               :tech-vault-aws-path)
                                              {:re-request-time-ms 4000}))

  ((:request-credentials-fn test-provider)))
