(ns backtype.storm.nimbus.leadership
  (:import [backtype.storm.nimbus NimbusLeadership])
  (:use [backtype.storm log]))

(defn nimbus-leadership [conf]
  (NimbusLeadership. conf))

(defn get-nimbus-leader-address [conf]
  (.getNimbusLeaderAddress (nimbus-leadership conf)))

(defn get-nimbus-hosts [conf]
  (.getNimbusHosts (nimbus-leadership conf)))

(defn acquire-leadership [nimbus-leadership]
    (when-let [nimbus-leader-address (.getNimbusLeaderAddress nimbus-leadership)]
      (log-message "Current Nimbus Leader: " nimbus-leader-address))
    (log-message "acquiring nimbus leadership...")
    (.acquireLeaderShip nimbus-leadership)
    (log-message "acuqired nimbus leadership!"))