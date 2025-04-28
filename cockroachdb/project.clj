(defproject cockroachdb "0.1.0"
  :description "Jepsen testing for CockroachDB"
  :url "http://cockroachlabs.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.9-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.postgresql/postgresql "9.4.1211"]
                 [jakarta.xml.bind/jakarta.xml.bind-api "2.3.3"]
                 [org.glassfish.jaxb/jaxb-runtime "2.3.3"]]
  :javac-options ["--release" "17"]
  :jvm-opts ["-Xmx12g"]
  :main jepsen.cockroach.runner
  :aot [jepsen.cockroach.runner
        clojure.tools.logging.impl])
