/**
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
package backtype.storm.utils;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ThreadResourceManager<T> {
    public static interface ResourceFactory<X> {
        X makeResource();
    }
    
    ResourceFactory<T> _factory;
    ConcurrentLinkedQueue<T> _resources = new ConcurrentLinkedQueue<T>();
    
    public ThreadResourceManager(ResourceFactory<T> factory) {
        _factory = factory;
    }
    
    public T acquire() {
        T ret = _resources.poll();
        if (ret == null) {
            ret = _factory.makeResource();
        }
        return ret;
    }
    
    public void release(T resource) {
        _resources.add(resource);
    }
}
