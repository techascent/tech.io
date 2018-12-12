(ns tech.io.auth
  "Authentication layer designed to work with hashicorp vault's aws credentialling system.
Given a function that takes no arguments but can produce a credential map, we want to store
the latest version of the map but also be prepared for the current credentials to time out
thus necessitating a new auth request.  In order to do this, providers need to throw exceptions
of the type:
  (ex-info \"Doesn't matter\" {:exception-action :request-credentials}
  This layer will then catch such exceptions and attempt threadsafe reauthentication."
  (:require [tech.io.protocols :as io-prot]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as c]
            [tech.io.cache :as cache]
            [tech.io.url :as url]))


(defn request-credentials
  [request-timeout-ms thread-chan]
  (let [result-chan (async/chan)
        result (when (async/>!! thread-chan {:result-chan result-chan})
                 (let [timeout-chan (async/timeout request-timeout-ms)]
                   (async/alt!!
                     timeout-chan :timeout
                     result-chan ([value] value))))]
    (cond
      (and result
           (not= result :timeout))
      result
      (= result :timeout)
      (throw (ex-info "Timeout request credentials" {:request-timeout-ms request-timeout-ms}))
      :else
      (throw (ex-info "Nil result requesting credentials" {})))))


(defn credential-thread
  [re-request-ms credential-fn]
  (let [control-chan (async/chan)
        thread-chan (async/thread
                      (loop [credentials nil
                             read-result (async/<!! control-chan)]
                        (if read-result
                          (let [{:keys [credentials error]}
                                (try
                                  {:credentials
                                   (if (or (nil? credentials)
                                           (> (- (System/currentTimeMillis)
                                                 (:access-time credentials))
                                              re-request-ms))
                                     (assoc (credential-fn)
                                            :access-time (System/currentTimeMillis))
                                     credentials)}
                                  (catch Throwable e
                                    {:error e}))
                                result-chan (:result-chan read-result)]
                            (if-not error
                              (async/>!! result-chan credentials)
                              (do
                                (log/warn (format "Error retrieving credentials: %s"
                                                  (with-out-str (println error))))
                                ;;Chill for a second to make sure we aren't spinning
                                (Thread/sleep 50)))
                            (recur credentials (async/<!! control-chan)))
                          (log/info "Credential request thread exiting"))))]
    {:shutdown-fn #(do (async/close! control-chan)
                       (async/<!! thread-chan))
     :request-chan control-chan}))


(defn start-auth-provider
  [provider]
  (if-not (:shutdown-fn provider)
    (let [{:keys [shutdown-fn request-chan]}
          (credential-thread (:re-request-time-ms provider)
                             (:src-cred-fn provider))]
      (assoc provider
             :request-credentials-fn #(request-credentials (:cred-request-timeout-ms provider)
                                                           request-chan)
             :shutdown-fn shutdown-fn))
    provider))


(defn stop-auth-provider
  [provider]
  (when-let [shutdown-fn (:shutdown-fn provider)]
    (shutdown-fn))
  (dissoc provider :request-credentials-fn :shutdown-fn))


(defrecord AuthProvider [cred-request-timeout-ms
                         re-request-time-ms
                         request-credentials-fn
                         src-cred-fn
                         src-provider]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (io-prot/input-stream src-provider url-parts (merge (request-credentials-fn) options)))
  (output-stream! [provider url-parts options]
    (io-prot/output-stream! src-provider url-parts (merge (request-credentials-fn) options)))
  (exists? [provider url-parts options]
    (io-prot/exists? src-provider url-parts (merge (request-credentials-fn) options)))
  (ls [provider url-parts options]
    (io-prot/ls src-provider url-parts (merge (request-credentials-fn) options)))
  (delete! [provider url-parts options]
    (io-prot/delete! src-provider url-parts (merge (request-credentials-fn) options)))
  (metadata [provider url-parts options]
    (io-prot/metadata src-provider url-parts (merge (request-credentials-fn) options)))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (io-prot/get-object src-provider url-parts (merge (request-credentials-fn) options)))
  (put-object! [provider url-parts value options]
    (io-prot/put-object! src-provider url-parts value (merge (request-credentials-fn) options)))


  c/Lifecycle
  (start [this]
    (start-auth-provider this))

  (stop [this]
    (stop-auth-provider this)))


(defn auth-provider
  "You need to call com.stuartsierra.component/start on this to enable the credential request system."
  [cred-fn {:keys [cred-request-timeout-ms ;;How long credentials last
                   re-request-time-ms ;;How long to wait for a credential request before signalling error.
                   src-provider]
            :or {cred-request-timeout-ms 10000
                 ;;Save credentials for 20 minutes
                 re-request-time-ms (* 20 60 1000)
                 src-provider (cache/forwarding-provider :url-parts->provider io-prot/url-parts->provider)}}]
  (->AuthProvider cred-request-timeout-ms re-request-time-ms
                  nil cred-fn src-provider))


(defn get-vault-aws-creds
  [vault-path]
  (let [vault-data ((resolve 'tech.vault-clj.core/read-credentials) vault-path)]
    (if-let [data (get
                   ((resolve 'tech.vault-clj.core/read-credentials) vault-path)
                   "data")]
      (merge {:tech.aws/access-key (or (get data "access_key")
                                       (get data "AWS_ACCESS_KEY_ID"))
              :tech.aws/secret-key (or (get data "secret_key")
                                       (get data "AWS_SECRET_ACCESS_KEY"))}
             (when-let [token (or (get data "security_token")
                                  (get data "AWS_SESSION_TOKEN"))]
               {:tech.aws/session-token token}))
      (throw (ex-info "Vault access error" vault-data)))))


(defn vault-aws-auth-provider
  [vault-path options]
  (require 'tech.vault-clj.core)
  (auth-provider #(get-vault-aws-creds vault-path options) options))
