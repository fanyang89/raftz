(ns raftz.register
  (:require [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [knossos.model :as model]
            [raftz.cli :as cli]))

(defrecord Client []
  client/Client
  (open! [this _ _] this)
  (setup! [this _] this)
  (invoke! [_ test op]
    (let [[k v] (:value op)
          id (cli/request-id)
          result (cli/invoke! (:nodes test) (:f op) k v id)]
      (cond-> (merge op (dissoc result :read-value) {:request-id id})
        (contains? result :read-value)
        (assoc :value (independent/tuple k (:read-value result))))))
  (teardown! [_ _])
  (close! [_ _]))

(defn operation [phase k f v]
  {:type :invoke :f f :value (independent/tuple k v) :phase phase})

(defn workload []
  (gen/stagger 0.1
    (gen/mix [(fn [] (operation :workload (rand-int 8) :read nil))
              (fn [] (operation :workload (rand-int 8) :write (rand-int 5)))
              (fn [] (operation :workload (rand-int 8) :cas
                                   [(rand-int 5) (rand-int 5)]))])))

(defn probe [phase key]
  (apply gen/phases
    (map #(gen/clients (gen/once %))
         [(operation phase key :write 1)
          (operation phase key :cas [1 2])
          (operation phase key :cas [1 3])
          (operation phase key :read nil)])))

(defn linear-checker []
  (independent/checker
    (checker/linearizable {:model (model/cas-register 0) :algorithm :linear})))

(defn coverage-checker []
  (reify checker/Checker
    (check [_ test history _]
      (let [successes (frequencies
                       (for [op history :when (= :ok (:type op))]
                         [(:phase op) (:f op)]))
            missing (for [phase [:warmup :workload :recovery]
                          f [:read :write :cas]
                          :when (zero? (get successes [phase f] 0))]
                      [phase f])
            faults (filter #(and (= :nemesis (:process %))
                                 (= :info (:type %))
                                 (true? (get-in % [:value :applied]))) history)
            fault-intervals (for [[start stop] (partition 2 1 faults)
                                  :when (and (= :start (:f start))
                                             (= :stop (:f stop)))]
                              [(:time start) (:time stop)])
            active-invocations (count
                                 (filter
                                   (fn [op]
                                     (and (= :invoke (:type op))
                                          (= :workload (:phase op))
                                          (some (fn [[start stop]]
                                                  (< start (:time op) stop))
                                                fault-intervals)))
                                   history))
            fault-ok? (or (= :none (:fault test))
                          (pos? active-invocations))
            mismatch? (some #(and (= :cas (:f %)) (= :fail (:type %))
                                  (= :cas-mismatch (:error %))) history)]
        {:valid? (boolean (and (empty? missing) fault-ok? mismatch?))
         :successes successes :missing (vec missing)
         :fault-exercised? (boolean fault-ok?)
         :invocations-during-fault active-invocations
         :cas-mismatch-observed? (boolean mismatch?)}))))

(defn checker []
  (checker/compose {:linear (linear-checker)
                    :coverage (coverage-checker)
                    :stats (checker/stats)}))
