(defproject techascent/tech.io "2.1-SNAPSHOT"
  :description "IO abstractions to enable rapid research and prototyping."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [techascent/tech.resource "4.0"]
                 [me.raynes/fs "1.4.6"]
                 [com.taoensso/nippy "2.15.0-alpha1"]
                 [com.taoensso/timbre "4.10.0"]
                 [techascent/tech.config "0.3.5"]
                 [org.clojure/core.async "0.4.474"]
                 [com.stuartsierra/component "0.3.2"]])
