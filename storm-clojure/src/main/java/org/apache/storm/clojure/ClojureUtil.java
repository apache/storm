/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.clojure;

import clojure.lang.RT;

public class ClojureUtil {
    public static synchronized clojure.lang.IFn loadClojureFn(String namespace, String name) {
        try {
            clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception e) {
            //if playing from the repl and defining functions, file won't exist
        }
        return (clojure.lang.IFn) RT.var(namespace, name).deref();
    }
}
