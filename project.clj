(defproject techascent/tech.io "4.32-SNAPSHOT"
  :description "IO abstractions to enable rapid research, prototyping, and cross cloud
application development."
  :url "http://github.com/tech-ascent/tech.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.2" :scope "provided"]
                 [techascent/tech.resource "5.07"]
                 [com.taoensso/nippy "3.2.0"]
                 [babashka/fs "0.0.3"]
                 [techascent/tech.config "0.3.13"]
                 [com.cnuernber/charred "1.033"]]
  :profiles {:dev {:dependencies [
                                  ;;[techascent/vault-clj "0.2.21"]
                                  [amperity/vault-clj "0.7.0"]
                                  ]}
             :codox
             {:dependencies [[codox-theme-rdash "0.1.2"]]
              :plugins [[lein-codox "0.10.7"]]
              :codox {:project {:name "tech.io"}
                      :metadata {:doc/format :markdown}
                      :themes [:rdash]
                      :source-paths ["src"]
                      :output-path "docs"
                      :source-uri "https://github.com/techascent/tech.io/blob/master/{filepath}#L{line}"
                      :namespaces [tech.v3.io
                                   tech.v3.io.url
                                   tech.v3.io.temp-file]}}}
  :aliases {"codox" ["with-profile" "codox,dev" "codox"]}
  )
