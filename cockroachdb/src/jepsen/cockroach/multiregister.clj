(ns jepsen.cockroach.multiregister
  "Multiple atomic registers test

  Splits registers up into different tables to make sure they fall in
  different ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as cockroach]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [reconnect :as rc]
             [independent :as independent]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(def reg-count 5)
(def reg-range (range reg-count))

(def table-prefix "String prepended to all table names." "register_")
(defn id->table
  "Turns an id into a table name string"
  [id]
  (str table-prefix id))
(def table-names (map id->table reg-range))

(defn r
  "Read a random register."
  [_ _]
  (->> (take 1 (shuffle reg-range))
       (mapv (fn [id] [:read id nil]))
       (array-map :type :invoke, :f :txn, :value)))

(defn w
  "Write a random subset of registers."
  [_ _]
  (->> (util/random-nonempty-subset reg-range)
       (mapv (fn [id] [:write id (rand-int 10)]))
       (array-map :type :invoke, :f :txn, :value)))

(defrecord MultiAtomicClient [tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/client node)))

  (setup! [this test]
    ;; Everyone's gotta block until we've made the tables.
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (c/with-conn [c conn]
          (info "Creating tables" (pr-str table-names))
          (Thread/sleep 1000)
          (doseq [t table-names]
            (j/execute! c [(str "drop table if exists " t)]))
          (Thread/sleep 1000)
          (doseq [t table-names]
            (j/execute! c [(str "create table " t
                                " (ik int primary key, val int)")])
            (j/execute! c [(str "alter table " t " scatter")])
            (info "Created table" t))))))

  (invoke! [this test op]
    (c/with-idempotent-txn #{:read}
      (c/with-exception->op op
        (c/with-conn [c conn]
          (c/with-timeout
            (try
              (c/with-txn [c c]
                (let [[ik txn] (:value op)
                      txn' (mapv
                            (fn [[f id val]]
                              (let [t    (id->table id)
                                    val' (case f
                                           :read
                                           (-> c
                                               ; Look up and return current value
                                               (c/query [(str "select val from " t " where ik = ?") ik]
                                                        {:row-fn :val :timeout c/timeout-delay})
                                               first)

                                           :write
                                           (do
                                             ; Perform blind write on key, return value
                                             (c/execute! c [(str "upsert into " t " values (?, ?)") ik val])
                                             (cockroach/update-keyrange! test t ik)
                                             val))]
                                [f id val']))
                            txn)]
                  (assoc op :type :ok, :value (independent/tuple ik txn'))))
              (catch org.postgresql.util.PSQLException e
                (if (re-find #"ERROR: restart transaction" (.getMessage e))
                  ; Definitely failed
                  (assoc op :type :fail)
                  (throw e)))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (rc/close! conn)))

(defn test
  [opts]
  (cockroach/basic-test
   (merge
    {:name        "multi-register"
     :client      {:client (MultiAtomicClient. (atom false) nil)
                   :during (independent/concurrent-generator
                            (count (:nodes opts))
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w])
                                   (gen/stagger 1/100)
                                   (gen/limit 60))))}
     :model       (model/multi-register {})
     :checker     (checker/compose
                   {:perf   (checker/perf)
                    :details (independent/checker
                              (checker/compose
                               {:timeline     (timeline/html)
                                :linearizable (checker/linearizable)}))})}
    opts)))
