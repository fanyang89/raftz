(ns raftz.cli-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
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

(deftest execute-requires-one-result-object
  (doseq [results ["[null]" "[0]" "[true]" "[\"result\"]" "[[]]"
                   "[]" "[{},{}]" "null" "{}" "\"result\""]
          f [:write :cas]]
    (is (= {:type :info :error :malformed-response}
           (cli/completion f
             (response (str "{\"code\":\"EXECUTE_CODE_OK\",\"results\":" results "}"))))))
  (is (= {:type :fail :error :cas-mismatch}
         (cli/completion :cas
           (response "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[{}]}"))))
  (is (= {:type :info :error :unexpected-row-count}
         (cli/completion :write
           (response "{\"code\":\"EXECUTE_CODE_OK\",\"results\":[{}]}")))))

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
  (let [deleted (atom [])
        delete-file io/delete-file
        result (with-redefs [io/delete-file (fn [file silently]
                                             (swap! deleted conj file)
                                             (delete-file file silently))
                             slurp (fn [& _] (throw (AssertionError. "Process never started")))]
                 (cli/run-command ["/no-such-raftz-binary"] 30))]
    (is (:not-started? result))
    (doseq [f [:write :cas]]
      (is (= :fail (:type (cli/completion f (cli/parse-result result))))))
    (is (= 2 (count (distinct @deleted))))
    (is (every? #(not (.exists %)) @deleted)))
  (is (= 0 (:exit (cli/run-command ["true"] 1000)))))

(deftest process-output-io-failure-is-indeterminate
  (doseq [fail-on [1 2]]
    (let [reads (atom 0)
          deleted (atom [])
          delete-file io/delete-file
          read-file slurp
          result (with-redefs [slurp (fn [file]
                                      (if (= fail-on (swap! reads inc))
                                        (throw (java.io.IOException. "Output read failed"))
                                        (read-file file)))
                               io/delete-file (fn [file silently]
                                                (swap! deleted conj file)
                                                (delete-file file silently))]
                   (cli/run-command ["/usr/bin/true"] 1000))]
      (is (= fail-on @reads))
      (is (not (:not-started? result)))
      (is (= "Output read failed" (:err result)))
      (doseq [f [:write :cas]]
        (is (= :info (:type (cli/completion f (cli/parse-result result))))))
      (is (= 2 (count (distinct @deleted))))
      (is (every? #(not (.exists %)) @deleted)))))
