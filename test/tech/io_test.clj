(ns tech.io-test
  (:require [tech.io :as io]
            [tech.io.cache :as cache]
            [tech.io.redirect :as redirect]
            [tech.io.temp-file :as temp-file]
            [tech.resource :as resource]
            [me.raynes.fs :as fs]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [clojure.test :refer :all]
            [tech.io.providers :as providers])
  (:import [java.io File]))


(deftest file-cache-test
  (let [temp-dir
        (resource/with-resource-context
          (temp-file/with-temp-dir
            temp-dir
            (let [fcache (cache/create-file-cache temp-dir {})]
              (io/with-provider fcache
                (let [proj-file (slurp (io/input-stream "file://project.clj"))]
                  (spit (io/output-stream! (temp-file/watch-file-for-delete
                                            "file://project-2.clj")) proj-file)))
              (is (= (slurp (io/input-stream "file://project.clj"))
                     (slurp (io/input-stream "file://project-2.clj"))))
              (is (= (slurp (io/input-stream "file://project.clj"))
                     (slurp (io/input-stream (io-prot/url->redirect-url
                                              fcache
                                              "file://project-2.clj"))))))))]
    (is (not (io/exists? (str "file://" temp-dir))))
    (is (not (io/exists? "file://project-2.clj")))))



(deftest get-put-object-cache-test
  (resource/with-resource-context
    (temp-file/with-temp-dir
      temp-dir
      (let [fcache (cache/create-file-cache temp-dir {})]
        (io/with-provider fcache
          (let [proj-file (slurp (io/input-stream (io/get-object "file://project.clj")))]
            (io/put-object! (temp-file/watch-file-for-delete "file://project-2.clj")
                            (.getBytes proj-file))
            (is (instance? File (io/get-object "file://project-2.clj")))
            (is (= (url/string-seq->file-path [temp-dir "file" "project-2.clj"])
                   (.getPath (io/get-object "file://project-2.clj")))))
          (is (= (slurp (io/input-stream "file://project.clj"))
                 (slurp (io/input-stream "file://project-2.clj"))))
          (is (= (slurp (io/input-stream "file://project.clj"))
                 (slurp (io/input-stream (io-prot/url->redirect-url
                                          fcache
                                          "file://project-2.clj"))))))))))


(deftest file-redirect-test
  (let [proj-file (slurp (io/input-stream (io/get-object "file://project.clj")))]
    (resource/with-resource-context
      (temp-file/with-temp-dir
        temp-dir
        (io/with-provider (redirect/create-file-provider temp-dir {})
          (spit (io/output-stream! "file://project-2.clj") proj-file)
          (io/put-object! "file://project-3.clj" (.getBytes proj-file))
          (is (= proj-file
                 (slurp (io/input-stream (str temp-dir "/" "file" "/" "project-2.clj")))))
          (is (= proj-file
                 (slurp (io/input-stream (str temp-dir "/" "file" "/" "project-3.clj"))))))))))


;; (deftest s3-ls
;;   (io/enable-s3!)
;;   (temp-file/with-temp-dir
;;     temp-dir
;;     (io/with-provider (->> [(providers/vault-auth-provider nil {})
;;                             (providers/caching-provider temp-dir {})]
;;                            providers/provider-seq->wrapped-providers)
;;       (let [result (io/ls "s3://techascent.test/")
;;             first-file (->> result
;;                             (remove :directory?)
;;                             first
;;                             :url
;;                             io/get-object)]
;;         ;;Caching should always return a file.
;;         (is (instance? File first-file))
;;         (is (> (count result) 0))))))
