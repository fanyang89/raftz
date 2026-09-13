(ns raftz.cluster
  (:require [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]
            [raftz.cli :as cli]))

(defn control! [action]
  (c/exec :timeout :20 "/usr/local/bin/node-control" (name action)))

(defn heal! [test]
  (c/on-nodes test (fn [_ _] (control! :heal) (control! :start))))

(defn database []
  (reify
    db/DB
    (setup! [_ _ _] (control! :heal) (control! :start))
    (teardown! [_ _ _]
      (try (control! :heal) (finally (control! :stop))))
    db/Primary
    (primaries [_ _] [])
    (setup-primary! [_ test _] (cli/initialize! (:nodes test)))
    db/LogFiles
    (log-files [_ _ _] ["/data/raft-sqlite.log"])))

(defn fault-nemesis [fault]
  (reify nemesis/Nemesis
    (setup! [this test] (heal! test) this)
    (invoke! [_ test op]
      (case (:f op)
        :start
        (let [address (cli/leader (:nodes test))
              node (or (some #(when (= address (cli/endpoint %)) %) (:nodes test))
                       (rand-nth (:nodes test)))
              action (case fault :partition :partition :kill :kill)
              output (c/on-nodes test [node] (fn [_ _] (control! action)))]
          (assoc op :value {:applied true :node node :action action :output output}))
        :stop
        (assoc op :value {:applied true :action :heal-and-restart
                         :output (heal! test)})))
    (teardown! [_ test] (heal! test))))
