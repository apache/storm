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
(ns integration.org.apache.storm.trident.integration-test
  (:use [clojure test])
  (:import [org.apache.storm Testing LocalCluster$Builder LocalCluster LocalDRPC])
  (:import [org.apache.storm.trident.testing Split CountAsAggregator StringLength TrueFilter
            MemoryMapState$Factory FeederCommitterBatchSpout FeederBatchSpout])
  (:import [org.apache.storm.trident.state StateSpec])
  (:import [org.apache.storm.trident TridentTopology]
           [org.apache.storm.trident.operation.impl CombinerAggStateUpdater]
           [org.apache.storm.trident.operation Function]
           [org.apache.storm.trident.operation.builtin Count Sum Equals MapGet Debug FilterNull FirstN TupleCollectionGet]
           [org.apache.storm.tuple Fields]
           [org.json.simple.parser JSONParser]
           [org.json.simple JSONValue]
           [org.apache.storm Config])
  (:use [org.apache.storm log util config]))

(defn exec-drpc [^LocalDRPC drpc function-name args]
  (if-let [res (.execute drpc function-name args)]
    (clojurify-structure (JSONValue/parse res))))

(defmacro letlocals
  [& body]
  (let [[tobind lexpr] (split-at (dec (count body)) body)
        binded (vec (mapcat (fn [e]
                              (if (and (list? e) (= 'bind (first e)))
                                [(second e) (last e)]
                                ['_ e]
                                ))
                            tobind))]
    `(let ~binded
       ~(first lexpr))))

(deftest test-memory-map-get-tuples
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (FeederBatchSpout. ["sentence"]))
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
              (.groupBy (Fields. ["word"]))
              (.persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))
              (.parallelismHint 6)
              ))
        (-> topo
            (.newDRPCStream "all-tuples" drpc)
            (.broadcast)
            (.stateQuery word-counts (Fields. ["args"]) (TupleCollectionGet.) (Fields. ["word" "count"]))
            (.project (Fields. ["word" "count"])))
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (.feed feeder [["hello the man said"] ["the"]])
          (is (= #{["hello" 1] ["said" 1] ["the" 2] ["man" 1]}
                 (into #{} (exec-drpc drpc "all-tuples" "man"))))
          (.feed feeder [["the foo"]])
          (is (= #{["hello" 1] ["said" 1] ["the" 3] ["man" 1] ["foo" 1]}
                 (into #{} (exec-drpc drpc "all-tuples" "man")))))))))

(deftest test-word-count
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (FeederBatchSpout. ["sentence"]))
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
              (.groupBy (Fields. ["word"]))
              (.persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))
              (.parallelismHint 6)
              ))
        (-> topo
            (.newDRPCStream "words" drpc)
            (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
            (.groupBy (Fields. ["word"]))
            (.stateQuery word-counts (Fields. ["word"]) (MapGet.) (Fields. ["count"]))
            (.aggregate (Fields. ["count"]) (Sum.) (Fields. ["sum"]))
            (.project (Fields. ["sum"])))
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (.feed feeder [["hello the man said"] ["the"]])
          (is (= [[2]] (exec-drpc drpc "words" "the")))
          (is (= [[1]] (exec-drpc drpc "words" "hello")))
          (.feed feeder [["the man on the moon"] ["where are you"]])
          (is (= [[4]] (exec-drpc drpc "words" "the")))
          (is (= [[2]] (exec-drpc drpc "words" "man")))
          (is (= [[8]] (exec-drpc drpc "words" "man where you the")))
          )))))

;; this test reproduces a bug where committer spouts freeze processing when
;; there's at least one repartitioning after the spout
(deftest test-word-count-committer-spout
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (FeederCommitterBatchSpout. ["sentence"]))
        (.setWaitToEmit feeder false) ;;this causes lots of empty batches
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.parallelismHint 2)
              (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
              (.groupBy (Fields. ["word"]))
              (.persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))
              (.parallelismHint 6)
              ))
        (-> topo
            (.newDRPCStream "words" drpc)
            (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
            (.groupBy (Fields. ["word"]))
            (.stateQuery word-counts (Fields. ["word"]) (MapGet.) (Fields. ["count"]))
            (.aggregate (Fields. ["count"]) (Sum.) (Fields. ["sum"]))
            (.project (Fields. ["sum"])))
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (.feed feeder [["hello the man said"] ["the"]])
          (is (= [[2]] (exec-drpc drpc "words" "the")))
          (is (= [[1]] (exec-drpc drpc "words" "hello")))
          (Thread/sleep 1000) ;; this is necessary to reproduce the bug where committer spouts freeze processing
          (.feed feeder [["the man on the moon"] ["where are you"]])
          (is (= [[4]] (exec-drpc drpc "words" "the")))
          (is (= [[2]] (exec-drpc drpc "words" "man")))
          (is (= [[8]] (exec-drpc drpc "words" "man where you the")))
          (.feed feeder [["the the"]])
          (is (= [[6]] (exec-drpc drpc "words" "the")))
          (.feed feeder [["the"]])
          (is (= [[7]] (exec-drpc drpc "words" "the")))
          )))))


(deftest test-count-agg
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (-> topo
            (.newDRPCStream "numwords" drpc)
            (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
            (.aggregate (CountAsAggregator.) (Fields. ["count"]))
            (.parallelismHint 2) ;;this makes sure batchGlobal is working correctly
            (.project (Fields. ["count"])))
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (doseq [i (range 100)]
            (is (= [[1]] (exec-drpc drpc "numwords" "the"))))
          (is (= [[0]] (exec-drpc drpc "numwords" "")))
          (is (= [[8]] (exec-drpc drpc "numwords" "1 2 3 4 5 6 7 8")))
          )))))

(deftest test-split-merge
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "splitter" drpc)))
        (bind s1
          (-> drpc-stream
              (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
              (.project (Fields. ["word"]))))
        (bind s2
          (-> drpc-stream
              (.each (Fields. ["args"]) (StringLength.) (Fields. ["len"]))
              (.project (Fields. ["len"]))))

        (.merge topo [s1 s2])
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (is (Testing/multiseteq [[7] ["the"] ["man"]] (exec-drpc drpc "splitter" "the man")))
          (is (Testing/multiseteq [[5] ["hello"]] (exec-drpc drpc "splitter" "hello")))
          )))))

(deftest test-multiple-groupings-same-stream
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "tester" drpc)
                                   (.each (Fields. ["args"]) (TrueFilter.))))
        (bind s1
          (-> drpc-stream
              (.groupBy (Fields. ["args"]))
              (.aggregate (CountAsAggregator.) (Fields. ["count"]))))
        (bind s2
          (-> drpc-stream
              (.groupBy (Fields. ["args"]))
              (.aggregate (CountAsAggregator.) (Fields. ["count"]))))

        (.merge topo [s1 s2])
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (is (Testing/multiseteq [["the" 1] ["the" 1]] (exec-drpc drpc "tester" "the")))
          (is (Testing/multiseteq [["aaaaa" 1] ["aaaaa" 1]] (exec-drpc drpc "tester" "aaaaa")))
          )))))

(deftest test-multi-repartition
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "tester" drpc)
                                   (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
                                   (.localOrShuffle)
                                   (.shuffle)
                                   (.aggregate (CountAsAggregator.) (Fields. ["count"]))
                                   ))
        (with-open [storm-topo (.submitTopology cluster "testing" {} (.build topo))]
          (is (Testing/multiseteq [[2]] (exec-drpc drpc "tester" "the man")))
          (is (Testing/multiseteq [[1]] (exec-drpc drpc "tester" "aaa")))
          )))))

(deftest test-stream-projection-validation
  (with-open [cluster (LocalCluster. )]
    (letlocals
     (bind feeder (FeederCommitterBatchSpout. ["sentence"]))
     (bind topo (TridentTopology.))
     ;; valid projection fields will not throw exceptions
     (bind word-counts
           (-> topo
               (.newStream "tester" feeder)
               (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
               (.groupBy (Fields. ["word"]))
               (.persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))
               (.parallelismHint 6)
               ))
     (bind stream (-> topo
                      (.newStream "tester" feeder)))
     ;; test .each
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence1"]) (Split.) (Fields. ["word"])))))
     ;; test .groupBy
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
                      (.groupBy (Fields. ["word1"])))))
     ;; test .aggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
                      (.groupBy (Fields. ["word"]))
                      (.aggregate (Fields. ["word1"]) (Count.) (Fields. ["count"])))))
     ;; test .project
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.project (Fields. ["sentence1"])))))
     ;; test .partitionBy
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.partitionBy (Fields. ["sentence1"])))))
     ;; test .partitionAggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
                      (.partitionAggregate (Fields. ["word1"]) (Count.) (Fields. ["count"])))))
     ;; test .persistentAggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
                      (.groupBy (Fields. ["word"]))
                      (.persistentAggregate (StateSpec. (MemoryMapState$Factory.)) (Fields. ["non-existent"]) (Count.) (Fields. ["count"])))))
     ;; test .partitionPersist
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
                      (.groupBy (Fields. ["word"]))
                      (.partitionPersist (StateSpec. (MemoryMapState$Factory.))
                                         (Fields. ["non-existent"])
                                         (CombinerAggStateUpdater. (Count.))
                                         (Fields. ["count"])))))
     ;; test .stateQuery
     (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
       (is (thrown? IllegalArgumentException
                    (-> topo
                        (.newDRPCStream "words" drpc)
                        (.each (Fields. ["args"]) (Split.) (Fields. ["word"]))
                        (.groupBy (Fields. ["word"]))
                        (.stateQuery word-counts (Fields. ["word1"]) (MapGet.) (Fields. ["count"]))))))
     )))


(deftest test-set-component-resources
  (with-open [cluster (LocalCluster. )]
    (with-open [drpc (LocalDRPC. (.getMetricRegistry cluster))]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (FeederBatchSpout. ["sentence"]))
        (bind add-bang (reify Function
                         (execute [_ tuple collector]
                           (. collector emit (str (. tuple getString 0) "!")))))
        (bind word-counts
          (.. topo
              (setResourceDefaults (doto (org.apache.storm.trident.operation.DefaultResourceDeclarer.)
                                     (.setMemoryLoad 0 0)
                                     (.setCPULoad 0)))
              (newStream "words" feeder)
              (parallelismHint 5)
              (setCPULoad 20)
              (setMemoryLoad 512 256)
              (each (Fields. ["sentence"]) (Split.) (Fields. ["word"]))
              (setCPULoad 10)
              (setMemoryLoad 512)
              (each (Fields. ["word"]) add-bang (Fields. ["word!"]))
              (parallelismHint 10)
              (setCPULoad 50)
              (setMemoryLoad 1024)
              (groupBy (Fields. ["word!"]))
              (persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))
              (setCPULoad 100)
              (setMemoryLoad 2048)))
        (with-open [storm-topo (.submitTopology cluster "testing" {TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 4096} (.build topo))]

          (let [parse-fn (fn [[k v]]
                           [k (clojurify-structure (. (JSONParser.) parse (.. v get_common get_json_conf)))])
                json-confs (into {} (map parse-fn (. storm-topo get_bolts)))]
            (testing "spout memory"
              (is (= (-> (json-confs "spout-words")
                         (get TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB))
                     512.0))

              (is (= (-> (json-confs "spout-words")
                         (get TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB))
                   256.0))

              (is (= (-> (json-confs "$spoutcoord-spout-words")
                         (get TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB))
                     512.0))

              (is (= (-> (json-confs "$spoutcoord-spout-words")
                         (get TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB))
                     256.0)))

            (testing "spout CPU"
              (is (= (-> (json-confs "spout-words")
                         (get TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT))
                     20.0))

              (is (= (-> (json-confs "$spoutcoord-spout-words")
                       (get TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT))
                     20.0)))

            (testing "bolt combinations"
              (is (= (-> (json-confs "b-1")
                         (get TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB))
                     (+ 1024.0 512.0)))

              (is (= (-> (json-confs "b-1")
                         (get TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT))
                     60.0)))

            (testing "aggregations after partition"
              (is (= (-> (json-confs "b-0")
                         (get TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB))
                     2048.0))

              (is (= (-> (json-confs "b-0")
                         (get TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT))
                     100.0)))))))))


