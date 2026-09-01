(ns jepsen.cockroach.auto-test
  (:require [clojure.test :refer :all]
            [jepsen.cockroach.auto :as auto]))

(deftest startcmd-without-extra-arguments-preserves-the-regular-command-test
  (let [cmd (flatten
             (auto/startcmd
              {:nodes ["n1" "n2"] :linearizable false}
              "n1"))]
    (is (some #{"--join=n2"} cmd))
    (is (not-any? #(and (string? %) (.startsWith % "--store")) cmd))
    (is (not-any? #(and (string? %) (.startsWith % "--locality")) cmd))))

(deftest startcmd-includes-only-the-current-nodes-extra-arguments-test
  (let [cmd (flatten
             (auto/startcmd
              {:nodes ["n1" "n2"]
               :linearizable false
               :cockroach-start-args
               {"n1" ["--store=n1" "--locality=zone=z1"]
                "n2" ["--store=n2" "--locality=zone=z2"]}}
              "n1"))]
    (is (some #{"--join=n2"} cmd))
    (is (some #{"--store=n1"} cmd))
    (is (some #{"--locality=zone=z1"} cmd))
    (is (not-any? #{"--store=n2" "--locality=zone=z2"} cmd))))
