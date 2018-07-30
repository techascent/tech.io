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


(defn with-credential-update
  "Attempt an s3 operation.  If the operation fails with an invalid
  access key, request new aws credentials from vault exactly once.
  Continue attempting operation until it either fails with a different
  exception, hits the timeout, or succeeds"
  [timeout-ms cred-request-fn execute-fn]
  (let [start-time (System/currentTimeMillis)]
     (loop [current-time (System/currentTimeMillis)
            credentials (cred-request-fn)]
       (let [{:keys [retval error]}
             (try
               {:retval (execute-fn credentials)}
               (catch Throwable e
                 {:error e}))]
         (if (and error
                  (= :request-credentials
                     (:exception-action (ex-data error)))
                  (< (- current-time start-time) timeout-ms))
           (recur (System/currentTimeMillis) (cred-request-fn))
           (if error
             (throw error)
             retval))))))



(defn request-credentials
  [request-timeout-ms thread-chan]
  (let [result-chan (async/chan)
        result (when (async/>!! thread-chan {:result-chan result-chan})
                 (let [timeout-chan (async/timeout request-timeout-ms)]
                   (async/alt!!
                     timeout-chan :timeout
                     result-chan ([value] value))))]
    (if (and result
             (not= result :timeout))
      result
      (throw (ex-info "Result channel was closed" {})))))


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
                                (log/error error)
                                (log/warn "Error retrieving credentials")
                                ;;Chill for a second to make sure we aren't spinning
                                (Thread/sleep 50)))
                            (recur credentials (async/<!! control-chan)))
                          (log/info "Credential request thread exiting"))))]
    {:shutdown-fn #(do (async/close! control-chan)
                       (async/<!! thread-chan))
     :request-chan control-chan}))


(defrecord AuthProvider [propagation-ms cred-request-timeout-ms re-request-time-ms
                         request-credentials-fn
                         src-cred-fn
                         src-provider]
  io-prot/IOProvider
  (input-stream [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/input-stream src-provider url-parts (merge % options))))
  (output-stream! [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/output-stream! src-provider url-parts (merge % options))))
  (exists? [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/exists? src-provider url-parts (merge % options))))
  (ls [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/ls src-provider url-parts (merge % options))))
  (delete! [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/delete! src-provider url-parts (merge % options))))
  (metadata [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/metadata src-provider url-parts (merge % options))))

  io-prot/ICopyObject
  (get-object [provider url-parts options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/get-object src-provider url-parts (merge % options))))
  (put-object! [provider url-parts value options]
    (with-credential-update cred-request-timeout-ms request-credentials-fn
      #(io-prot/put-object! src-provider url-parts value (merge % options))))


  c/Lifecycle
  (start [this]
    (if (:shutdown-fn this)
      this
      (let [{:keys [shutdown-fn request-chan]} (credential-thread re-request-time-ms src-cred-fn)]
        (assoc this
               :request-credentials-fn #(request-credentials cred-request-timeout-ms request-chan)
               :shutdown-fn shutdown-fn))))

  (stop [this]
    ((:shutdown-fn this))
    (dissoc this :request-credentials-fn :shutdown-fn)))



(defn auth-provider
  "You need to call com.stuartsierra.component/start on this to enable the credential request system."
  [cred-fn {:keys [cred-propagation-ms
                   cred-request-timeout-ms
                   re-request-time-ms
                   src-provider]
            :or {cred-propagation-ms 50
                 cred-request-timeout-ms 2000
                 ;;Save credentials for 20 minutes
                 re-request-time-ms (* 20 60 1000)
                 src-provider (cache/forwarding-provider :url-parts->provider io-prot/url-parts->provider)}}]
  (->AuthProvider cred-propagation-ms cred-request-timeout-ms re-request-time-ms
                  nil cred-fn src-provider))


(defn get-vault-aws-creds
  [vault-path]
  (when-let [data (get
                   ((resolve 'tech.vault-clj.core/read-credentials) vault-path)
                   "data")]
    (merge {:tech.io.s3/access-key (get data "access_key")
            :tech.io.s3/secret-key (get data "secret_key")}
           (when-let [token (get data "security_token")]
             {:tech.io.s3/session-token token}))))


(defn vault-aws-auth-provider
  [{:keys [vault-path]
    :or {vault-path "aws/sts/core"}
    :as options}]
  (require 'tech.vault-clj.core)
  (auth-provider #(get-vault-aws-creds vault-path) options))
