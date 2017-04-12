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

(ns org.apache.storm.util
  (:import [java.util Map List HashMap])
  (:import [org.apache.storm.generated ErrorInfo])
  (:import [org.apache.storm.utils Utils])
  (:import [java.util List Set])
  (:use [clojure walk])
  (:use [org.apache.storm log]))

;; name-with-attributes by Konrad Hinsen:
(defn name-with-attributes
  "To be used in macro definitions.
  Handles optional docstrings and attribute maps for a name to be defined
  in a list of macro arguments. If the first macro argument is a string,
  it is added as a docstring to name and removed from the macro argument
  list. If afterwards the first macro argument is a map, its entries are
  added to the name's metadata map and the map is removed from the
  macro argument list. The return value is a vector containing the name
  with its extended metadata map and the list of unprocessed macro
  arguments."
  [name macro-args]
  (let [[docstring macro-args] (if (string? (first macro-args))
                                 [(first macro-args) (next macro-args)]
                                 [nil macro-args])
        [attr macro-args] (if (map? (first macro-args))
                            [(first macro-args) (next macro-args)]
                            [{} macro-args])
        attr (if docstring
               (assoc attr :doc docstring)
               attr)
        attr (if (meta name)
               (conj (meta name) attr)
               attr)]
    [(with-meta name attr) macro-args]))

(defmacro defnk
  "Define a function accepting keyword arguments. Symbols up to the first
  keyword in the parameter list are taken as positional arguments.  Then
  an alternating sequence of keywords and defaults values is expected. The
  values of the keyword arguments are available in the function body by
  virtue of the symbol corresponding to the keyword (cf. :keys destructuring).
  defnk accepts an optional docstring as well as an optional metadata map."
  [fn-name & fn-tail]
  (let [[fn-name [args & body]] (name-with-attributes fn-name fn-tail)
        [pos kw-vals] (split-with symbol? args)
        syms (map #(-> % name symbol) (take-nth 2 kw-vals))
        values (take-nth 2 (rest kw-vals))
        sym-vals (apply hash-map (interleave syms values))
        de-map {:keys (vec syms) :or sym-vals}]
    `(defn ~fn-name
       [~@pos & options#]
       (let [~de-map (apply hash-map options#)]
         ~@body))))

(defmacro thrown-cause?
  [klass & body]
  `(try
     ~@body
     false
     (catch Throwable t#
       (let [tc# (Utils/exceptionCauseIsInstanceOf ~klass t#)]
         (if (not tc#) (log-error t# "Exception did not match " ~klass))
         tc#))))

(defn clojurify-structure
  [s]
  (if s
    (prewalk (fn [x]
             (cond (instance? Map x) (into {} x)
                   (instance? List x) (vec x)
                   (instance? Set x) (into #{} x)
                   ;; (Boolean. false) does not evaluate to false in an if.
                   ;; This fixes that.
                   (instance? Boolean x) (boolean x)
                   true x))
           s)))
; move this func form convert.clj due to cyclic load dependency
(defn clojurify-error [^ErrorInfo error]
  (if error
    {
      :error (.get_error error)
      :time-secs (.get_error_time_secs error)
      :host (.get_host error)
      :port (.get_port error)
      }
    ))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn map-val
  [afn amap]
  (into {}
        (for [[k v] amap]
          [k (afn v)])))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn filter-key
  [afn amap]
  (into {} (filter (fn [[k v]] (afn k)) amap)))

;TODO: Once all the other clojure functions (100+ locations) are translated to java, this function becomes moot.
(def not-nil? (complement nil?))

(defmacro dofor [& body]
  `(doall (for ~@body)))

(defmacro -<>
  ([x] x)
  ([x form] (if (seq? form)
              (with-meta
                (let [[begin [_ & end]] (split-with #(not= % '<>) form)]
                  (concat begin [x] end))
                (meta form))
              (list form x)))
  ([x form & more] `(-<> (-<> ~x ~form) ~@more)))

(defn hashmap-to-persistent [^HashMap m]
  (zipmap (.keySet m) (.values m)))
