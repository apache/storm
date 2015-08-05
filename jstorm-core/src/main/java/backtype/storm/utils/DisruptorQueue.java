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

import backtype.storm.metric.api.IStatefulObject;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is the ability to catch up to the producer by processing tuples in batches.
 */
public abstract class DisruptorQueue implements IStatefulObject {
    public static void setUseSleep(boolean useSleep) {
        DisruptorQueueImpl.setUseSleep(useSleep);
    }
    
    private static boolean CAPACITY_LIMITED = false;
    
    public static void setLimited(boolean limited) {
        CAPACITY_LIMITED = limited;
    }
    
    public static DisruptorQueue mkInstance(String queueName, ProducerType producerType, int bufferSize, WaitStrategy wait) {
        if (CAPACITY_LIMITED == true) {
            return new DisruptorQueueImpl(queueName, producerType, bufferSize, wait);
        } else {
            return new DisruptorWrapBlockingQueue(queueName, producerType, bufferSize, wait);
        }
    }
    
    public abstract String getName();
    
    public abstract void haltWithInterrupt();
    
    public abstract Object poll();
    
    public abstract Object take();
    
    public abstract void consumeBatch(EventHandler<Object> handler);
    
    public abstract void consumeBatchWhenAvailable(EventHandler<Object> handler);
    
    public abstract void publish(Object obj);
    
    public abstract void publish(Object obj, boolean block) throws InsufficientCapacityException;
    
    public abstract void consumerStarted();
    
    public abstract void clear();
    
    public abstract long population();
    
    public abstract long capacity();
    
    public abstract long writePos();
    
    public abstract long readPos();
    
    public abstract float pctFull();
    
}
