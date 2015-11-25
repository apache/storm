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
package com.alibaba.jstorm.common.metric.old;

import com.alibaba.jstorm.common.metric.old.operator.convert.Convertor;
import com.alibaba.jstorm.common.metric.old.operator.merger.AvgMerger;
import com.alibaba.jstorm.common.metric.old.operator.updater.AvgUpdater;
import com.alibaba.jstorm.common.metric.old.window.Metric;

/**
 * Meter is used to compute tps
 * 
 * Attention: 1.
 * 
 * @author zhongyan.feng
 * 
 */
public class Histogram extends Metric<Double, Histogram.HistorgramPair> {
    private static final long serialVersionUID = -1362345159511508074L;

    public Histogram() {
        defaultValue = new HistorgramPair();
        updater = new AvgUpdater();
        merger = new AvgMerger();
        convertor = new HistogramConvertor();

        init();
    }

    public static class HistogramConvertor implements Convertor<HistorgramPair, Double> {
        private static final long serialVersionUID = -1569170826785657226L;

        @Override
        public Double convert(HistorgramPair from) {
            // TODO Auto-generated method stub
            if (from == null) {
                return 0.0d;
            }

            if (from.getTimes() == 0) {
                return 0.0d;
            } else {
                return from.getSum() / from.getTimes();
            }
        }

    }

    public static class HistorgramPair {
        private double sum;
        private long times;

        public HistorgramPair() {

        }

        public HistorgramPair(double sum, long times) {
            this.sum = sum;
            this.times = times;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public void addValue(double value) {
            sum += value;
        }

        public long getTimes() {
            return times;
        }

        public void setTimes(long times) {
            this.times = times;
        }

        public void addTimes(long time) {
            times += time;
        }
    }

}
