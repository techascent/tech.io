(ns tech.io-test
  (:require [tech.io :as io]
            [tech.io.temp-file :as temp-file]
            [tech.resource :as resource]
            [clojure.test :refer [deftest is]])
  (:import [java.io File]))


(deftest interlocked-copy-test
  (resource/stack-resource-context
   (temp-file/with-temp-dir
     temp-dir
     (let [dest-file (io/interlocked-copy-to-file
                      "project.clj"
                      (str temp-dir "/" "test.clj"))]
       (is (= (slurp (io/input-stream "file://project.clj"))
              (slurp (io/input-stream dest-file))))))))
