(ns tech.io.test
  (:require [tech.io :as io]
            [tech.io.cache :as cache]
            [me.raynes.fs :as fs]
            [tech.io.url :as url]
            [tech.io.protocols :as io-prot]
            [clojure.test :refer :all])
  (:import [java.io File]))


(deftest file-cache-test
  (let [fcache (cache/create-file-cache "testdir" {})]
    (with-bindings {#'io/*provider* fcache}
      (let [proj-file (slurp (io/input-stream "file://project.clj"))
            _ (spit (io/output-stream! "file://project-2.clj") proj-file)]))
    (is (= (slurp (io/input-stream "file://project.clj"))
           (slurp (io/input-stream "file://project-2.clj"))))
    (is (= (slurp (io/input-stream "file://testdir/file/project.clj"))
           (slurp (io/input-stream "file://testdir/file/project-2.clj"))))
    (fs/delete-dir "testdir")
    (fs/delete "project-2.clj")))



(deftest get-put-object-cache-test
  (let [fcache (cache/create-file-cache "testdir" {})]
    (with-bindings {#'io/*provider* fcache}
      (let [proj-file (slurp (io/input-stream (io/get-object "file://project.clj")))]
        (io/put-object! "file://project-2.clj" (.getBytes proj-file))
        (is (instance? File (io/get-object "file://project-2.clj")))
        (is (= "testdir/file/project-2.clj" (.getPath (io/get-object "file://project-2.clj"))))))
    (is (= (slurp (io/input-stream "file://project.clj"))
           (slurp (io/input-stream "file://project-2.clj"))))
    (is (= (slurp (io/input-stream "file://testdir/file/project.clj"))
           (slurp (io/input-stream "file://testdir/file/project-2.clj"))))
    (fs/delete-dir "testdir")
    (fs/delete "project-2.clj")))
