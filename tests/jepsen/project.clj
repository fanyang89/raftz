(defproject raftz-jepsen "0.1.0"
  :description "Jepsen CAS registers over the raftz raft-sqlite CLI"
  :url "https://github.com/fanyang89/raftz"
  :license {:name "MIT" :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.6"]
                 [knossos "0.3.8"]
                 [cheshire "5.10.0"]]
  :main raftz.core
  :jvm-opts ["-Xmx2g" "-Djava.awt.headless=true"])
