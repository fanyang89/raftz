(ns raftz.core
  (:require [jepsen.core :as jepsen]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [raftz.cluster :as cluster]
            [raftz.register :as register]))

(defn test-map [fault seconds]
  (merge tests/noop-test
    {:name (str "raftz-register-" (name fault))
     :nodes ["n1" "n2" "n3"]
     :concurrency 6
     :ssh {:username "root" :private-key-path "/keys/id_rsa"
           :strict-host-key-checking false}
     :fault fault
     :db (cluster/database)
     :client (register/->Client)
     :nemesis (if (= :none fault) nemesis/noop (cluster/fault-nemesis fault))
     :checker (register/checker)
     :generator
     (gen/phases
       (register/probe :warmup 8)
       (->> (register/workload)
            (gen/nemesis
              (when-not (= :none fault)
                (cycle [(gen/sleep 3) {:type :info :f :start}
                        (gen/sleep 4) {:type :info :f :stop}])))
            (gen/time-limit seconds))
       (gen/nemesis (gen/once {:type :info :f :stop}))
       (gen/sleep 5)
       (register/probe :recovery 8))}))

(defn -main [& [fault-arg seconds-arg]]
  (try
    (let [fault (keyword (or fault-arg "none"))
          seconds (Long/parseLong (or seconds-arg "20"))]
      (when-not (and (#{:none :partition :kill} fault) (<= 15 seconds 300))
        (throw (ex-info "Usage: lein run none|partition|kill [15..300 seconds]" {})))
      (let [result (jepsen/run! (test-map fault seconds))]
        (shutdown-agents)
        (System/exit (if (true? (get-in result [:results :valid?])) 0 1))))
    (catch Throwable e
      (.printStackTrace e)
      (shutdown-agents)
      (System/exit 2))))
