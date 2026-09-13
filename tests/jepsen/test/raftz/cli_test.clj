(ns raftz.cli-test
  (:require [clojure.test :refer :all]
            [raftz.cli :as cli]))

(defn response [s] (cli/parse-result {:exit 0 :out s :err ""}))

(deftest protobuf-json-responses
  (is (= {:type :ok :read-value 42}
         (cli/completion :read (response "{\"rows\":[{\"values\":[{\"integerValue\":\"42\"}]}]}"))))
  (is (= :ok (:type (cli/completion :write
                     (response "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[{\"rowsAffected\":\"1\"}]}")))))
  (is (= {:type :fail :error :cas-mismatch}
         (cli/completion :cas
           (response "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[{}]}"))))
  (is (= :info (:type (cli/completion :write
                       (response "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[{}]}")))))
  (is (= :fail (:type (cli/completion :write
                       (response "{\"code\":\"EXECUTE_CODE_SQL_ERROR\"}")))))
  (is (= :fail (:type (cli/completion :read (response "{\"rows\":[]}")))))
  (doseq [s ["not json" "[]" "{}" "{\"code\":\"UNKNOWN\"}"
             "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[]}"]]
    (is (= :info (:type (cli/completion :write (response s)))))))

(deftest uncertain-writes-are-never-definite-failures
  (doseq [result [{:exit 124 :out "" :err "" :timeout? true}
                  {:exit 1 :out "" :err "connection reset"}
                  {:exit 1 :out "" :err "RPC failed: unavailable: service unavailable\n"}
                  {:exit 1 :out "" :err "RPC failed: failed_precondition: not leader\n"}]
          f [:write :cas]]
    (is (= :info (:type (cli/completion f (cli/parse-result result)))))
    (is (= :fail (:type (cli/completion :read (cli/parse-result result))))))
  (is (= "failed_precondition"
         (:rpc-code (cli/parse-result
                      {:exit 1 :out "" :err "RPC failed: failed_precondition: not leader\n"}))))
  (is (= :fail (:type (cli/completion :write {:error :not-started}))))
  (is (= :fail (:type (cli/completion :write {:error :no-leader})))))

(deftest one-submission-and-stable-request-id
  (let [calls (atom [])]
    (with-redefs [cli/leader (constantly "10.0.0.1:8001")
                  cli/call! (fn [args _] (swap! calls conj args) {:error :timeout})]
      (is (= :info (:type (cli/invoke! ["n1" "n2"] :cas 7 [2 3] "identity")))))
    (is (= [["exec" "10.0.0.1:8001" "identity"
             "UPDATE registers SET val = ?1 WHERE id = ?2 AND val = ?3"
             "int:3" "int:7" "int:2"]] @calls))))

(deftest process-deadline-and-launch-failure
  (is (:timeout? (cli/run-command ["sleep" "5"] 30)))
  (is (:not-started? (cli/run-command ["/no-such-raftz-binary"] 30)))
  (is (= 0 (:exit (cli/run-command ["true"] 1000)))))
