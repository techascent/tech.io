(defproject techascent/tech.io "2.11-SNAPSHOT"
  :description "IO abstractions to enable rapid research and prototyping."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:dev {:lein-tools-deps/config {:resolve-aliases [:test]}}}
  :repositories {"releases"  {:url "s3p://techascent.jars/releases/"
                              :no-auth true
                              :sign-releases false}})
