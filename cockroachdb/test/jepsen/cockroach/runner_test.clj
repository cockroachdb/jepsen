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

(deftest parse-cockroach-env-test
  (let [env ["COCKROACH_EXAMPLE_SETTING=true" "EMPTY=" "WITH_EQUALS=a=b"]]
    (is (= {:options {:cockroach-env env}}
           (runner/parse-cockroach-env {:options {:cockroach-env env}})))))

(deftest parse-cockroach-env-rejects-invalid-values-test
  (doseq [[env msg] [[["NOVALUE"] #"expected KEY=VALUE"]
                     [["1BAD=x"] #"expected KEY=VALUE"]
                     [["=x"] #"expected KEY=VALUE"]
                     [["COCKROACH_MAX_OFFSET=1s"] #"cannot override COCKROACH_MAX_OFFSET"]
                     [["A=1" "A=2"] #"Duplicate --cockroach-env A"]]]
    (is (thrown-with-msg?
         IllegalArgumentException msg
         (runner/parse-cockroach-env {:options {:cockroach-env env}})))))
