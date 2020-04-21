(defproject techascent/tech.io "3.18-SNAPSHOT"
  :description "IO abstractions to enable rapid research and prototyping."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:dev {:dependencies [
                                  ;;[techascent/vault-clj "0.2.21"]
                                  [amperity/vault-clj "0.7.0"]
                                  ]}}

  ;; To test tech vault provider
  ;; :repositories {"releases"  {:url "s3p://techascent.jars/releases/"
  ;;                             :no-auth true}}
  )
