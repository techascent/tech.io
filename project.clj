(defproject techascent/tech.io "0.1.7"
  :description "IO abstractions to enable rapid research and prototyping."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [thinktopic/think.resource "1.2.1"]
                 [amazonica "0.3.127"
                  :exclusions [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor
                               com.fasterxml.jackson.core/jackson-core
                               com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client
                               com.amazonaws/dynamodb-streams-kinesis-adapter]]
                 ;; Only incude S3 for now as that is the only service this
                 ;; project is using
                 ;; see: https://github.com/mcohen01/amazonica#for-the-memory-constrained
                 [com.amazonaws/aws-java-sdk-core "1.11.341"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.341"]
                 [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor "2.9.0"]
                 [com.fasterxml.jackson.core/jackson-databind "2.9.0"]
                 [me.raynes/fs "1.4.6"]
                 [com.taoensso/nippy "2.14.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [techascent/tech.config "0.3.5"]])
