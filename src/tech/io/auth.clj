(ns tech.io.auth
  "Authentication layer designed to work with hashicorp vault's aws credentialling
  system.  Given a function that takes no arguments but can produce a credential map, we
  want to store the latest version of the map but also be prepared for the current
  credentials to time out thus necessitating a new auth request.  In order to do this,
  providers need to throw exceptions of the type:
  (ex-info \"Doesn't matter\" {:exception-action :request-credentials}
  This layer will then catch such exceptions and attempt threadsafe reauthentication."
  (:require [tech.io.protocols :as io-prot]
            [clojure.tools.logging :as log]))


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



(defn auth-provider
  "Create an auth provider.  The basic requirement is a credential function that, called
  with no arguments, does whatever is necessary to either get credentials or error out
  returning a map of credentials.  If provided keys are already not nil and in the map
  then the credential function isn't called.
  Arguments:
  :credential-timeout-ms - timeout of credential system.
  :provided-keys - sequence of keys provided by credential function."
  [cred-fn {:keys [credential-timeout-ms
                   provided-auth-keys]
            :or {credential-timeout-ms (* 20 60 1000)}}]

  (let [cred-atom (atom {})]
    (reify
      io-prot/IOAuth
      (authenticate [this url-parts options]
        (if (and (seq provided-auth-keys)
                 (every? #(contains? options %) provided-auth-keys))
          options
          (merge options
                 (get-credentials credential-timeout-ms cred-fn cred-atom)))))))


(defn authenticated-provider
  "Wrap an io-provider such that the options map is augmented with authentication
  information."
  [src-io-provider auth-provider]
  (reify
    io-prot/IOProvider
    (input-stream [provider url-parts options]
      (io-prot/input-stream src-io-provider url-parts
                            (io-prot/authenticate auth-provider url-parts options)))
    (output-stream! [provider url-parts options]
      (io-prot/output-stream! src-io-provider url-parts
                              (io-prot/authenticate auth-provider url-parts options)))
    (exists? [provider url-parts options]
      (io-prot/exists? src-io-provider url-parts
                       (io-prot/authenticate auth-provider url-parts options)))
    (ls [provider url-parts options]
      (io-prot/ls src-io-provider url-parts
                  (io-prot/authenticate auth-provider url-parts options)))
    (delete! [provider url-parts options]
      (io-prot/delete! src-io-provider url-parts
                       (io-prot/authenticate auth-provider url-parts options)))
    (metadata [provider url-parts options]
      (io-prot/metadata src-io-provider url-parts
                        (io-prot/authenticate auth-provider url-parts options)))

    io-prot/ICopyObject
    (get-object [provider url-parts options]
      (io-prot/get-object src-io-provider url-parts
                          (io-prot/authenticate auth-provider url-parts options)))
    (put-object! [provider url-parts value options]
      (io-prot/put-object! src-io-provider url-parts value
                           (io-prot/authenticate auth-provider url-parts options)))))

;;Wrapper for techascent vault-clj - old but not yet busted
(def tech-read-cred-fn
  (delay (try
           (let [tech-fn
                 (locking #'authenticated-provider
                   (requiring-resolve
                    'tech.vault-clj.core/read-credentials))]
             (fn [vault-path]
               (-> (tech-fn vault-path)
                   (get "data")
                   (#(->> (map (fn [[k v]]
                                 [(keyword k) v])
                               %)
                          (into {}))))))
           (catch Throwable e e))))

;;Wrapper for amperity - new hotness
(def amperity-read-cred-fn
  (delay (try
           (locking #'authenticated-provider
             (requiring-resolve
              `tech.io.amperity-vault/read-credentials))
           (catch Throwable e e))))


(defn read-credentials
  [vault-path]
  (let [cred-fns [@tech-read-cred-fn
                  @amperity-read-cred-fn]]
    (when (every? #(instance? Throwable %) cred-fns)
      (log/warnf "Failed to load either techascent vault-clj: %s
 or amperity vault-clj %s"
                 (.getMessage ^Throwable (first cred-fns))
                 (.getMessage ^Throwable (second cred-fns)))
      {})
    (when-let [vault-fn (->> cred-fns
                             (remove #(instance? Throwable %))
                             first)]
      (try
        (vault-fn vault-path)
        (catch Throwable e
          (log/warnf "Failed to get vault credentials from path %s:%s" vault-path e)
          nil)))))


(def aws-auth-required-keys [:tech.aws/access-key
                             :tech.aws/secret-key])


(defn get-vault-aws-creds
  [vault-path options]
  (log/debug (format "Request vault information: %s" vault-path))
  (if-let [data (read-credentials vault-path)]
    (merge {:tech.aws/access-key (or (get data :access_key)
                                     (get data :AWS_ACCESS_KEY_ID))
            :tech.aws/secret-key (or (get data :secret_key)
                                     (get data :AWS_SECRET_ACCESS_KEY))}
           (when-let [token (or (get data :security_token)
                                (get data :AWS_SESSION_TOKEN))]
             {:tech.aws/session-token token}))
    {}))


(defn vault-aws-auth-provider
  [vault-path & [options]]
  (let [options (assoc options :provided-auth-keys
                       (or (:provided-auth-keys options)
                           aws-auth-required-keys))]
    (auth-provider #(get-vault-aws-creds vault-path options)
                   options)))


(comment

  (def test-provider (vault-aws-auth-provider "bad/vault/path"
                                              {:re-request-time-ms 1000}))

  (io-prot/authenticate test-provider {} {:a 1})

  (def test-provider (vault-aws-auth-provider (tech.config.core/get-config
                                               :tech-vault-aws-path)
                                              {:re-request-time-ms 4000}))

  (io-prot/authenticate test-provider {} {}))
