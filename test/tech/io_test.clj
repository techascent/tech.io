(ns tech.io.test
  (:require [tech.io :as io]
            [me.raynes.fs :as fs]
            [tech.io.url :as url]
            [clojure.test :refer :all]))


(deftest file-cache-test
  (let [fcache (io/create-file-cache "testdir" {})]
    (with-bindings {#'io/*provider* fcache}
      (let [proj-file (slurp (io/input-stream "file://project.clj"))
            _ (spit (io/output-stream! "file://project-2.clj") proj-file)]))
    (is (= (slurp (io/input-stream "file://project.clj"))
           (slurp (io/input-stream "file://project-2.clj"))))
    (is (= (slurp (io/input-stream "file://testdir/file/project.clj"))
           (slurp (io/input-stream "file://testdir/file/project-2.clj"))))
    (fs/delete-dir "testdir")
    (fs/delete "project-2.clj")))
