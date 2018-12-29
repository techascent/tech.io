(defproject techascent/tech.io "2.5-SNAPSHOT"
  :description "IO abstractions to enable rapid research and prototyping."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [techascent/tech.resource "4.0"]
                 [clj-commons/fs "1.5.0"]
                 [com.taoensso/nippy "2.15.0-alpha1"]
                 [com.taoensso/timbre "4.10.0"]
                 [techascent/tech.config "0.3.5"]
                 [techascent/tech.parallel "1.2"]]
  :profiles {:dev {:dependencies [[techascent/vault-clj "0.2.17"]]}})
