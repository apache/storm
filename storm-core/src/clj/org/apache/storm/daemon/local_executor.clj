(ns org.apache.storm.daemon.local-executor
  (:use [org.apache.storm util config log])
  (:import [org.apache.storm.tuple AddressedTuple])
  (:import [org.apache.storm.utils DisruptorQueue])
  (:import [org.apache.storm Config Constants]))

(defn mk-executor-transfer-fn [batch-transfer->worker storm-conf]
  (fn this
    [task tuple]
    (let [val (AddressedTuple. task tuple)]
      (when (= true (storm-conf TOPOLOGY-DEBUG))
        (log-message "TRANSFERING tuple " val))
      (.publish ^DisruptorQueue batch-transfer->worker val))))
