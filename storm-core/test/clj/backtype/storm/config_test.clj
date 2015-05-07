;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.config-test
  (:require [backtype.storm.config :as c]
            [backtype.storm.util :as util]
            [clojure.test :refer :all])
  (:import [backtype.storm ConfigValidation]
           [backtype.storm.utils Utils]))

(deftest test-validity
  (is (Utils/isValidConf {c/TOPOLOGY-DEBUG true "q" "asasdasd" "aaa" (Integer. "123") "bbb" (Long. "456") "eee" [1 2 (Integer. "3") (Long. "4")]}))
  (is (not (Utils/isValidConf {"qqq" (backtype.storm.utils.Utils.)})))
  )

(deftest test-power-of-2-validator
  (let [validator ConfigValidation/PowerOf2Validator]
    (doseq [x [42.42 42 23423423423 -33 -32 -1 -0.00001 0 -0 "Forty-two"]]
      (is (util/thrown-cause? java.lang.IllegalArgumentException
        (.validateField validator "test" x))))

    (doseq [x [64 4294967296 1 nil]]
      (is (nil? (try
                  (.validateField validator "test" x)
                  (catch Exception e e)))))))

(deftest test-list-validator
  (let [validator ConfigValidation/StringsValidator]
    (doseq [x [
               ["Forty-two" 42]
               [42]
               [true "false"]
               [nil]
               [nil "nil"]
              ]]
      (is (util/thrown-cause-with-msg?
            java.lang.IllegalArgumentException #"(?i).*each element.*"
        (.validateField validator "test" x))))

    (doseq [x ["not a list at all"]]
      (is (util/thrown-cause-with-msg?
            java.lang.IllegalArgumentException #"(?i).*must be an iterable.*"
        (.validateField validator "test" x))))

    (doseq [x [
               ["one" "two" "three"]
               [""]
               ["42" "64"]
               nil
              ]]
    (is (nil? (try
                (.validateField validator "test" x)
                (catch Exception e e)))))))

(deftest test-integer-validator
  (let [validator ConfigValidation/IntegerValidator]
    (.validateField validator "test" nil)
    (.validateField validator "test" 1000)
    (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" 1.34)))
    (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" (inc Integer/MAX_VALUE))))))

(deftest test-integers-validator
  (let [validator ConfigValidation/IntegersValidator]
    (.validateField validator "test" nil)
    (.validateField validator "test" [1000 0 -1000])
    (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" [0 10 1.34])))
    (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" [0 nil])))
    (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" [-100 (inc Integer/MAX_VALUE)])))))

(deftest test-double-validator
  (let [validator ConfigValidation/DoubleValidator]
    (.validateField validator "test" nil)
    (.validateField validator "test" 10)
    ;; we can provide lenient way to convert int/long to double with losing precision
    (.validateField validator "test" Integer/MAX_VALUE)
    (.validateField validator "test" (inc Integer/MAX_VALUE))
    (.validateField validator "test" Double/MAX_VALUE)))

(deftest test-topology-workers-is-integer
  (let [validator (c/CONFIG-SCHEMA-MAP c/TOPOLOGY-WORKERS)]
    (.validateField validator "test" 42)
    (is (util/thrown-cause? java.lang.IllegalArgumentException
      (.validateField validator "test" 3.14159)))))

(deftest test-topology-stats-sample-rate-is-float
  (let [validator (c/CONFIG-SCHEMA-MAP c/TOPOLOGY-STATS-SAMPLE-RATE)]
    (.validateField validator "test" 0.5)
    (.validateField validator "test" 10)
    (.validateField validator "test" Double/MAX_VALUE)))

(deftest test-isolation-scheduler-machines-is-map
  (let [validator (c/CONFIG-SCHEMA-MAP c/ISOLATION-SCHEDULER-MACHINES)]
    (is (nil? (try
                (.validateField validator "test" {})
                (catch Exception e e))))
    (is (nil? (try
                (.validateField validator "test" {"host0" 1 "host1" 2})
                (catch Exception e e))))
    (is (util/thrown-cause? java.lang.IllegalArgumentException
      (.validateField validator "test" 42)))))

(deftest test-positive-integer-validator
  (let [validator ConfigValidation/PositiveIntegerValidator]
    (doseq [x [42.42 -32 0 -0 "Forty-two"]]
      (is (util/thrown-cause? java.lang.IllegalArgumentException
        (.validateField validator "test" x))))

    (doseq [x [42 4294967296 1 nil]]
      (is (nil? (try
                  (.validateField validator "test" x)
                  (catch Exception e e)))))))

(deftest test-worker-childopts-is-string-or-string-list
  (let [pass-cases [nil "some string" ["some" "string" "list"]]]
    (testing "worker.childopts validates"
      (let [validator (c/CONFIG-SCHEMA-MAP c/WORKER-CHILDOPTS)]
        (doseq [value pass-cases]
          (is (nil? (try
                      (.validateField validator "test" value)
                      (catch Exception e e)))))
        (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" 42)))))

    (testing "topology.worker.childopts validates"
      (let [validator (c/CONFIG-SCHEMA-MAP c/TOPOLOGY-WORKER-CHILDOPTS)]
        (doseq [value pass-cases]
          (is (nil? (try
                      (.validateField validator "test" value)
                      (catch Exception e e)))))
        (is (util/thrown-cause? java.lang.IllegalArgumentException
          (.validateField validator "test" 42)))))))
