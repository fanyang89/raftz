(ns raftz.cluster-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as control]
            [jepsen.nemesis :as nemesis]
            [raftz.cli :as cli]
            [raftz.cluster :as cluster]))

(deftest fault-targets-test-node-and-heals-all-nodes
  (let [commands (atom [])
        nodes ["n1" "n2" "n3"]
        test {:nodes nodes}]
    (with-redefs [cli/leader (constantly "n2:8001")
                  cli/endpoint #(str % ":8001")
                  cluster/control! #(swap! commands conj %)
                  control/on-nodes (fn
                                     ([t f] (control/on-nodes t (:nodes t) f))
                                     ([t ns f] (into {} (for [n ns] [n (f t n)]))))]
      (doseq [[fault action] [[:partition :partition] [:kill :kill]]]
        (reset! commands [])
        (let [n (cluster/fault-nemesis fault)
              start (nemesis/invoke! n test {:type :info :f :start})]
          (is (= "n2" (get-in start [:value :node])))
          (is (true? (get-in start [:value :applied])))
          (is (= [action] @commands))
          (nemesis/teardown! n test)
          (is (= [action :heal :start :heal :start :heal :start] @commands)))))))

(deftest initialization-retries-keep-identity
  (let [calls (atom [])]
    (with-redefs [cli/leader (constantly "n1:8001")
                  cli/call! (fn [args _]
                              (swap! calls conj args)
                              (if (= 1 (count @calls))
                                {:error :timeout}
                                {:body {:code "EXECUTE_CODE_OK"}}))]
      (cli/initialize! ["n1"]))
    (is (= 3 (count @calls)))
    (is (= (first @calls) (second @calls)))
    (is (not= (nth (second @calls) 2) (nth (last @calls) 2)))))
