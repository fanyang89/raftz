(ns raftz.cli
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.util UUID)
           (java.util.concurrent TimeUnit)))

(def binary "/usr/local/bin/raft-sqlite")

(defn request-id [] (str (UUID/randomUUID)))

(defn run-command [argv timeout-ms]
  (let [out (java.io.File/createTempFile "raftz-out-" ".log")
        err (java.io.File/createTempFile "raftz-err-" ".log")]
    (try
      (let [builder (-> (ProcessBuilder. ^java.util.List (vec argv))
                        (.redirectOutput out)
                        (.redirectError err))
            [process launch-error] (try
                                     [(.start builder) nil]
                                     (catch java.io.IOException e
                                       [nil (.getMessage e)]))]
        (if process
          (try
            (let [done? (.waitFor process timeout-ms TimeUnit/MILLISECONDS)]
              (when-not done?
                (.destroyForcibly process)
                (.waitFor process))
              {:exit (.exitValue process) :out (slurp out) :err (slurp err)
               :timeout? (not done?)})
            (catch java.io.IOException e
              {:exit -1 :out "" :err (.getMessage e)})
            (finally
              (when (.isAlive process) (.destroyForcibly process))))
          {:exit -1 :out "" :err launch-error :not-started? true}))
      (finally (io/delete-file out true) (io/delete-file err true)))))

(defn parse-result [{:keys [exit out err timeout? not-started?]}]
  (cond
    not-started? {:error :not-started}
    timeout? {:error :timeout}
    (not= 0 exit) {:error :rpc-or-transport
                   :rpc-code (second (re-find #"(?m)^RPC failed: ([a-z_]+):" err))
                   :detail err}
    :else (try
            (let [body (json/parse-string out true)]
              (if (map? body) {:body body} {:error :malformed-response}))
            (catch Exception _ {:error :malformed-response}))))

(defn call! [args timeout-ms]
  (parse-result (run-command (into [binary] args) timeout-ms)))

(defn endpoint [node]
  (str (.getHostAddress (java.net.InetAddress/getByName node)) ":8001"))

(defn leader [nodes]
  (some (fn [node]
          (let [address (endpoint node)
                result (call! ["status" address] 1500)]
            (when (= "leader" (some-> result :body :role str/lower-case))
              address)))
        (shuffle nodes)))

(defn integer-value [v]
  (cond
    (integer? v) v
    (and (string? v) (re-matches #"-?[0-9]+" v)) (Long/parseLong v)
    :else (throw (ex-info "Expected protobuf integer" {:value v}))))

(defn completion [f {:keys [body error] :as result}]
  (if error
    {:type (if (or (= :read f) (= :not-started error) (= :no-leader error))
             :fail :info)
     :error (dissoc result :body)}
    (try
      (if (= :read f)
        (let [rows (:rows body)
              values (:values (first rows))]
          (when-not (and (= 1 (count rows)) (= 1 (count values))
                         (contains? (first values) :integerValue))
            (throw (ex-info "Expected exactly one register value" {})))
          {:type :ok :read-value (integer-value (:integerValue (first values)))})
        (case (:code body)
          "EXECUTE_CODE_OK"
          (let [results (:results body)]
            (when-not (and (vector? results) (= 1 (count results))
                           (map? (first results)))
              (throw (ex-info "Expected one statement result object" {})))
            (let [n (integer-value (get (first results) :rowsAffected "0"))]
              (cond
                (= 1 n) {:type :ok}
                (and (= :cas f) (= 0 n)) {:type :fail :error :cas-mismatch}
                :else {:type :info :error :unexpected-row-count})))
          "EXECUTE_CODE_SQL_ERROR" {:type :fail :error :sql-error}
          "EXECUTE_CODE_INVALID_REQUEST" {:type :fail :error :invalid-request}
          "EXECUTE_CODE_REQUEST_CONFLICT" {:type :info :error :request-conflict}
          {:type :info :error :unexpected-execute-code}))
      (catch Exception _
        {:type (if (= :read f) :fail :info) :error :malformed-response}))))

(defn arguments [address id f k v]
  (case f
    :read ["query" address "SELECT val FROM registers WHERE id = ?1" (str "int:" k)]
    :write ["exec" address id "UPDATE registers SET val = ?1 WHERE id = ?2"
            (str "int:" v) (str "int:" k)]
    :cas (let [[old new] v]
           ["exec" address id "UPDATE registers SET val = ?1 WHERE id = ?2 AND val = ?3"
            (str "int:" new) (str "int:" k) (str "int:" old)])))

(defn invoke! [nodes f k v id]
  (if-let [address (leader nodes)]
    (completion f (call! (arguments address id f k v) 4000))
    (completion f {:error :no-leader})))

(defn initialize! [nodes]
  (doseq [sql ["CREATE TABLE IF NOT EXISTS registers (id INTEGER PRIMARY KEY, val INTEGER NOT NULL) STRICT"
               "WITH RECURSIVE keys(id) AS (VALUES(0) UNION ALL SELECT id+1 FROM keys WHERE id<8) INSERT OR IGNORE INTO registers SELECT id,0 FROM keys"]]
    (let [id (request-id)]
      (loop [attempt 0]
        (let [result (when-let [address (leader nodes)]
                       (call! ["exec" address id sql] 4000))]
          (when-not (= "EXECUTE_CODE_OK" (get-in result [:body :code]))
            (if (< attempt 20)
              (do (Thread/sleep 500) (recur (inc attempt)))
              (throw (ex-info "Register initialization failed" {:result result})))))))))
