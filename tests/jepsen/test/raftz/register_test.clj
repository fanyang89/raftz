(ns raftz.register-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.core :as jepsen]
            [jepsen.independent :as independent]
            [raftz.core :as core]
            [raftz.register :as register]))

(defn history [operations]
  (vec (map-indexed
         (fn [i op] (assoc op :index i :time (* 1000000 i)))
         (mapcat (fn [[process f input type output]]
                   [{:process process :f f :type :invoke
                     :value (independent/tuple 0 input)}
                    {:process process :f f :type type
                     :value (independent/tuple 0 output)}]) operations))))

(defn check-linear [label operations]
  (checker/check (register/linear-checker)
    (jepsen/prepare-test {:name (str "raftz-unit-" label) :nodes []})
    (history operations) {}))

(deftest real-knossos-checker-semantics
  (is (true? (:valid? (check-linear "valid-cas"
                       [[0 :write 1 :ok 1]
                        [0 :cas [1 2] :ok [1 2]]
                        [0 :cas [1 3] :fail [1 3]]
                        [0 :read nil :ok 2]]))))
  (is (false? (:valid? (check-linear "invalid-stale-read"
                        [[0 :write 1 :ok 1] [0 :read nil :ok 0]]))))
  (is (false? (:valid? (check-linear "invalid-cas"
                        [[0 :read nil :ok 0] [0 :cas [1 2] :ok [1 2]]]))))
  (is (true? (:valid? (check-linear "unknown-write"
                       [[0 :write 1 :info 1] [1 :read nil :ok 1]])))))

(deftest coverage-rejects-vacuous-or-unexercised-tests
  (let [check #(checker/check (register/coverage-checker) {:fault %1} %2 {})
        successes (for [phase [:warmup :workload :recovery] f [:read :write :cas]]
                    {:type :ok :phase phase :f f})
        covered (conj (vec successes) {:type :fail :f :cas :error :cas-mismatch})
        faults [{:process :nemesis :type :info :f :start :time 1 :value {:applied true}}
                {:process :nemesis :type :info :f :stop :time 3 :value {:applied true}}]]
    (is (false? (:valid? (check :none []))))
    (is (true? (:valid? (check :none covered))))
    (is (false? (:valid? (check :partition covered))))
    (is (false? (:valid? (check :partition (concat covered faults)))))
    (is (true? (:valid? (check :partition
                         (concat covered faults
                           [{:type :invoke :phase :workload :f :read :time 2}])))))
    (is (false? (:valid? (check :none (remove #(= :recovery (:phase %)) covered)))))
    (is (false? (:valid? (check :none (remove #(= :cas (:f %)) covered)))))))

(deftest fixed-disposable-topology
  (doseq [fault [:none :partition :kill]]
    (let [test (core/test-map fault 20)]
      (is (= ["n1" "n2" "n3"] (:nodes test)))
      (is (= 6 (:concurrency test)))
      (is (= fault (:fault test))))))
