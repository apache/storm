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

(ns backtype.storm.daemon.drpc
  (:require [backtype.storm.util :as util]
            [backtype.storm.ui.helpers :as helpers]
            [backtype.storm.config :as config]
            [backtype.storm.daemon.common :as common]
            [backtype.storm.log :refer [log-debug log-warn log-message]]
            [ring.middleware.reload :as reload]
            [compojure.core :refer [routes POST GET]])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftConnectionType ReqContext]
           [backtype.storm.security.auth.authorizer DRPCAuthorizerBase]
           [backtype.storm.generated DistributedRPC$Iface DistributedRPC$Processor
                                     DRPCRequest DRPCExecutionException DistributedRPCInvocations$Iface
                                     DistributedRPCInvocations$Processor]
           [java.util.concurrent Semaphore ConcurrentLinkedQueue]
           [backtype.storm.daemon Shutdownable]
           [java.net InetAddress]
           [backtype.storm.generated AuthorizationException])
  (:gen-class))

(defn timeout-check-secs [] 5)

(defn acquire-queue [queues-atom function]
  (swap! queues-atom
    (fn [amap]
      (if-not (amap function)
        (assoc amap function (ConcurrentLinkedQueue.))
        amap)))
  (@queues-atom function))

(defn check-authorization
  ([aclHandler mapping operation context]
    (if aclHandler
      (let [context (or context (ReqContext/context))]
        (if-not (.permit aclHandler context operation mapping)
          (let [principal (.principal context)
                user (if principal (.getName principal) "unknown")]
              (throw (AuthorizationException.
                       (str "DRPC request '" operation "' for '"
                            user "' user is not authorized"))))))))
  ([aclHandler mapping operation]
    (check-authorization aclHandler mapping operation (ReqContext/context))))

;; TODO: change this to use TimeCacheMap
(defn service-handler [conf]
  (let [drpc-acl-handler (common/mk-authorization-handler (conf config/DRPC-AUTHORIZER) conf)
        ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        id->function (atom {})
        id->request (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                  (swap! id->result dissoc id)
                  (swap! id->function dissoc id)
                  (swap! id->request dissoc id)
                  (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))  ;; TODO: is this needed?
        clear-thread (util/async-loop
                       (fn []
                         (doseq [[id start] @id->start]
                           (when (> (util/time-delta start) (conf config/DRPC-REQUEST-TIMEOUT-SECS))
                             (when-let [sem (@id->sem id)]
                               (.remove (acquire-queue request-queues (@id->function id)) (@id->request id))
                               (log-warn "Timeout DRPC request id: " id " start at " start)
                               (.release sem))
                             (cleanup id)))
                         (timeout-check-secs)))]
    (reify DistributedRPC$Iface
      (^String execute
        [this ^String function ^String args]
        (log-debug "Received DRPC request for " function " (" args ") at " (System/currentTimeMillis))
        (check-authorization drpc-acl-handler
                             {DRPCAuthorizerBase/FUNCTION_NAME function}
                             "execute")
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. args id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues function)]
          (swap! id->start assoc id (util/current-time-secs))
          (swap! id->sem assoc id sem)
          (swap! id->function assoc id function)
          (swap! id->request assoc id req)
          (.add queue req)
          (log-debug "Waiting for DRPC result for " function " " args " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired DRPC result for " function " " args " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning DRPC result for " function " " args " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              (if (nil? result)
                (throw (DRPCExecutionException. "Request timed out"))
                result)))))

      DistributedRPCInvocations$Iface

      (^void result
        [this ^String id ^String result]
        (when-let [func (@id->function id)]
          (check-authorization drpc-acl-handler
                               {DRPCAuthorizerBase/FUNCTION_NAME func}
                               "result")
          (let [^Semaphore sem (@id->sem id)]
            (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
            (when sem
              (swap! id->result assoc id result)
              (.release sem)
              ))))

      (^void failRequest
        [this ^String id]
        (when-let [func (@id->function id)]
          (check-authorization drpc-acl-handler
                               {DRPCAuthorizerBase/FUNCTION_NAME func}
                               "failRequest")
          (let [^Semaphore sem (@id->sem id)]
            (when sem
              (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
              (.release sem)))))

      (^DRPCRequest fetchRequest
        [this ^String func]
        (check-authorization drpc-acl-handler
                             {DRPCAuthorizerBase/FUNCTION_NAME func}
                             "fetchRequest")
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for " func " at " (System/currentTimeMillis))
              ret)
            (DRPCRequest. "" ""))))

      Shutdownable

      (shutdown
        [this]
        (.interrupt clear-thread)))))

(defn handle-request [handler]
  (fn [request]
    (handler request)))

(defn webapp [handler http-creds-handler]
  (->
    (routes
      (POST "/drpc/:func" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (if http-creds-handler
            (.populateContext http-creds-handler (ReqContext/context)
                              servlet-request))
          (.execute handler func args)))
      (POST "/drpc/:func/" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (if http-creds-handler
            (.populateContext http-creds-handler (ReqContext/context)
                              servlet-request))
          (.execute handler func args)))
      (GET "/drpc/:func/:args" [:as {:keys [servlet-request]} func args & m]
          (if http-creds-handler
            (.populateContext http-creds-handler (ReqContext/context)
                              servlet-request))
          (.execute handler func args))
      (GET "/drpc/:func/" [:as {:keys [servlet-request]} func & m]
          (if http-creds-handler
            (.populateContext http-creds-handler (ReqContext/context)
                              servlet-request))
          (.execute handler func ""))
      (GET "/drpc/:func" [:as {:keys [servlet-request]} func & m]
          (if http-creds-handler
            (.populateContext http-creds-handler (ReqContext/context)
                              servlet-request))
          (.execute handler func "")))
    (reload/wrap-reload '[backtype.storm.daemon.drpc])
    handle-request))

(defn launch-server!
  ([]
    (let [conf (config/read-storm-config)
          worker-threads (int (conf config/DRPC-WORKER-THREADS))
          queue-size (int (conf config/DRPC-QUEUE-SIZE))
          drpc-http-port (int (conf config/DRPC-HTTP-PORT))
          drpc-port (int (conf config/DRPC-PORT))
          drpc-service-handler (service-handler conf)
          ;; requests and returns need to be on separate thread pools, since calls to
          ;; "execute" don't unblock until other thrift methods are called. So if
          ;; 64 threads are calling execute, the server won't accept the result
          ;; invocations that will unblock those threads
          handler-server (when (> drpc-port 0)
                           (ThriftServer. conf
                             (DistributedRPC$Processor. drpc-service-handler)
                             ThriftConnectionType/DRPC))
          invoke-server (ThriftServer. conf
                          (DistributedRPCInvocations$Processor. drpc-service-handler)
                          ThriftConnectionType/DRPC_INVOCATIONS)
          http-creds-handler (AuthUtils/GetDrpcHttpCredentialsPlugin conf)]
      (util/add-shutdown-hook-with-force-kill-in-1-sec (fn []
                                                    (if handler-server (.stop handler-server))
                                                    (.stop invoke-server)))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (when (> drpc-http-port 0)
        (let [app (webapp drpc-service-handler http-creds-handler)
              filter-class (conf config/DRPC-HTTP-FILTER)
              filter-params (conf config/DRPC-HTTP-FILTER-PARAMS)
              filters-confs [{:filter-class filter-class
                              :filter-params filter-params}]
              https-port (int (conf config/DRPC-HTTPS-PORT))
              https-ks-path (conf config/DRPC-HTTPS-KEYSTORE-PATH)
              https-ks-password (conf config/DRPC-HTTPS-KEYSTORE-PASSWORD)
              https-ks-type (conf config/DRPC-HTTPS-KEYSTORE-TYPE)
              https-key-password (conf config/DRPC-HTTPS-KEY-PASSWORD)
              https-ts-path (conf config/DRPC-HTTPS-TRUSTSTORE-PATH)
              https-ts-password (conf config/DRPC-HTTPS-TRUSTSTORE-PASSWORD)
              https-ts-type (conf config/DRPC-HTTPS-TRUSTSTORE-TYPE)
              https-want-client-auth (conf config/DRPC-HTTPS-WANT-CLIENT-AUTH)
              https-need-client-auth (conf config/DRPC-HTTPS-NEED-CLIENT-AUTH)]

          (helpers/storm-run-jetty
           {:port drpc-http-port
            :configurator (fn [server]
                            (helpers/config-ssl server
                                        https-port
                                        https-ks-path
                                        https-ks-password
                                        https-ks-type
                                        https-key-password
                                        https-ts-path
                                        https-ts-password
                                        https-ts-type
                                        https-need-client-auth
                                        https-want-client-auth)
                            (helpers/config-filter server app filters-confs))})))
      (when handler-server
        (.serve handler-server)))))

(defn -main []
  (util/setup-default-uncaught-exception-handler)
  (launch-server!))
