(ns jepsen.cockroach.runner-test
  (:require [clojure.test :refer :all]
            [jepsen.cockroach.runner :as runner]))

(deftest parse-cockroach-start-args-test
  (is (= {:options
          {:nodes ["n1" "n2"]
           :cockroach-start-args
           {"n1" ["--store=type=basalt,path=basalt://tenant@cluster/store"
                  "--locality=cloud=gce,region=us-east1,zone=us-east1-b"]
            "n2" ["--temp-dir=/mnt/data1"]}}}
         (runner/parse-cockroach-start-args
          {:options
           {:nodes ["n1" "n2"]
            :cockroach-start-arg
            ["n1=--store=type=basalt,path=basalt://tenant@cluster/store"
             "n1=--locality=cloud=gce,region=us-east1,zone=us-east1-b"
             "n2=--temp-dir=/mnt/data1"]}}))))

(deftest parse-cockroach-start-args-rejects-invalid-values-test
  (is (thrown-with-msg?
       IllegalArgumentException
       #"expected NODE=ARG"
       (runner/parse-cockroach-start-args
        {:options {:nodes ["n1"] :cockroach-start-arg ["n1"]}})))
  (is (thrown-with-msg?
       IllegalArgumentException
       #"Unknown node"
       (runner/parse-cockroach-start-args
        {:options {:nodes ["n1"] :cockroach-start-arg ["n2=--foo"]}}))))
