(ns tech.io.url-test
  (:require [clojure.test :refer :all]
            [tech.io.url :as url]))


(deftest invalid-url-test
  ;;feel free to add
  (is (thrown? Throwable (url/url->parts "s3:/a/b/c")))
  (is (not (nil? (url/url->parts "s3://a/b/c"))))
  (is (not (nil? (url/url->parts "makeitup://a/b/c")))))
