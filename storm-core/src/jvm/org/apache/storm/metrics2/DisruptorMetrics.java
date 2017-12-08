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
package org.apache.storm.metrics2;

import org.apache.storm.utils.DisruptorQueue;

public class DisruptorMetrics {
    private final SimpleGauge<Long> capacity;
    private final SimpleGauge<Long> population;
    private final SimpleGauge<Long> writePosition;
    private final SimpleGauge<Long> readPosition;
    private final SimpleGauge<Double> arrivalRate;
    private final SimpleGauge<Double> sojournTime;
    private final SimpleGauge<Long> overflow;
    private final SimpleGauge<Float> pctFull;


    DisruptorMetrics(SimpleGauge<Long> capacity,
                    SimpleGauge<Long> population,
                    SimpleGauge<Long> writePosition,
                    SimpleGauge<Long> readPosition,
                    SimpleGauge<Double> arrivalRate,
                    SimpleGauge<Double> sojournTime,
                    SimpleGauge<Long> overflow,
                    SimpleGauge<Float> pctFull) {
        this.capacity = capacity;
        this.population = population;
        this.writePosition = writePosition;
        this.readPosition = readPosition;
        this.arrivalRate = arrivalRate;
        this.sojournTime = sojournTime;
        this.overflow = overflow;
        this.pctFull = pctFull;
    }

    public void setCapacity(Long capacity) {
        this.capacity.set(capacity);
    }

    public void setPopulation(Long population) {
        this.population.set(population);
    }

    public void setWritePosition(Long writePosition) {
        this.writePosition.set(writePosition);
    }

    public void setReadPosition(Long readPosition) {
        this.readPosition.set(readPosition);
    }

    public void setArrivalRate(Double arrivalRate) {
        this.arrivalRate.set(arrivalRate);
    }

    public void setSojournTime(Double soujournTime) {
        this.sojournTime.set(soujournTime);
    }

    public void setOverflow(Long overflow) {
        this.overflow.set(overflow);
    }

    public void setPercentFull(Float pctFull){
        this.pctFull.set(pctFull);
    }

    public void set(DisruptorQueue.QueueMetrics metrics){
        this.capacity.set(metrics.capacity());
        this.population.set(metrics.population());
        this.writePosition.set(metrics.writePos());
        this.readPosition.set(metrics.readPos());
        this.arrivalRate.set(metrics.arrivalRate());
        this.sojournTime.set(metrics.sojournTime());
        this.overflow.set(metrics.overflow());
        this.pctFull.set(metrics.pctFull());
    }
}
