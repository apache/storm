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
(ns backtype.storm.grouping-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter NGrouping])
  (:import [backtype.storm.grouping LoadMapping])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common executor])
  )

(bootstrap)

(deftest test-shuffle
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
                    {"2" (thrift/mk-bolt-spec {"1" :shuffle} (TestGlobalCount.)
                                            :parallelism-hint 6)
                     })
          results (complete-topology cluster
                                     topology
                                     ;; important for test that
                                     ;; #tuples = multiple of 4 and 6
                                     :mock-sources {"1" [["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                       ]}
                                     )]
      (is (= (* 6 4) (.size (read-tuples results "2")))))))

(deftest test-shuffle-load-even
 (let [shuffle-fn (mk-shuffle-grouper [1 2] {})
       num-messages 10000
       min-prcnt (int (* num-messages 0.47))
       max-prcnt (int (* num-messages 0.53))
       load (LoadMapping.)
       _ (.setLocal load {(int 1) 0.0 (int 2) 0.0})
       data [1 2]
       freq (frequencies (for [x (range 0 num-messages)] (shuffle-fn (int 1) data load)))
       load1 (.get freq (int 1))
       load2 (.get freq (int 2))]
    (log-message "FREQ:" freq)
    (is (>= load1 min-prcnt))
    (is (<= load1 max-prcnt))
    (is (>= load2 min-prcnt))
    (is (<= load2 max-prcnt))))

(deftest test-shuffle-load-uneven
 (let [shuffle-fn (mk-shuffle-grouper [1 2] {})
       num-messages 10000
       min1-prcnt (int (* num-messages 0.30))
       max1-prcnt (int (* num-messages 0.36))
       min2-prcnt (int (* num-messages 0.63))
       max2-prcnt (int (* num-messages 0.69))
       load (LoadMapping.)
       _ (.setLocal load {(int 1) 0.5 (int 2) 0.0})
       data [1 2]
       freq (frequencies (for [x (range 0 num-messages)] (shuffle-fn (int 1) data load)))
       load1 (.get freq (int 1))
       load2 (.get freq (int 2))]
    (log-message "FREQ:" freq)
    (is (>= load1 min1-prcnt))
    (is (<= load1 max1-prcnt))
    (is (>= load2 min2-prcnt))
    (is (<= load2 max2-prcnt))))

(defbolt id-bolt ["val"] [tuple collector]
  (emit-bolt! collector (.getValues tuple))
  (ack! collector tuple))

(deftest test-custom-groupings
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology
                    {"1" (spout-spec (TestWordSpout. true))}
                    {"2" (bolt-spec {"1" (NGrouping. 2)}
                                  id-bolt
                                  :p 4)
                     "3" (bolt-spec {"1" (JavaObject. "backtype.storm.testing.NGrouping"
                                                      [(JavaObjectArg/int_arg 3)])}
                                  id-bolt
                                  :p 6)
                     })
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [["a"]
                                                        ["b"]
                                                        ]}
                                     )]
      (is (ms= [["a"] ["a"] ["b"] ["b"]]
               (read-tuples results "2")))
      (is (ms= [["a"] ["a"] ["a"] ["b"] ["b"] ["b"]]
               (read-tuples results "3")))
      )))
