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

package org.apache.storm.loadgen;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stats related to something with a normal distribution, and a way to randomly simulate it.
 */
public class NormalDistStats implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NormalDistStats.class);
    public final double mean;
    public final double stddev;
    public final double min;
    public final double max;

    /**
     * Read the stats from a config.
     * @param conf the config.
     * @return the corresponding stats.
     */
    public static NormalDistStats fromConf(Map<String, Object> conf) {
        return fromConf(conf, null);
    }

    /**
     * Read the stats from a config.
     * @param conf the config.
     * @param def the default mean.
     * @return the corresponding stats.
     */
    public static NormalDistStats fromConf(Map<String, Object> conf, Double def) {
        if (conf == null) {
            conf = Collections.emptyMap();
        }
        double mean = ObjectReader.getDouble(conf.get("mean"), def);
        double stddev = ObjectReader.getDouble(conf.get("stddev"), mean / 4);
        double min = ObjectReader.getDouble(conf.get("min"), 0.0);
        double max = ObjectReader.getDouble(conf.get("max"), Double.MAX_VALUE);
        return new NormalDistStats(mean, stddev, min, max);
    }

    /**
     * Return this as a config.
     * @return the config version of this.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("mean", mean);
        ret.put("stddev", stddev);
        ret.put("min", min);
        ret.put("max", max);
        return ret;
    }

    /**
     * Create an instance of this from a list of values.  The metrics will be computed from the values.
     * @param values the values to compute metrics from.
     */
    public NormalDistStats(List<Double> values) {
        //Compute the stats for these and save them
        double min = values.isEmpty() ? 0.0 : values.get(0);
        double max = values.isEmpty() ? 0.0 : values.get(0);
        double sum = 0.0;
        long count = values.size();
        for (Double v: values) {
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max,v);
        }
        double mean = sum / Math.max(count, 1);
        double sdPartial = 0;
        for (Double v: values) {
            sdPartial += Math.pow(v - mean, 2);
        }
        double stddev = 0.0;
        if (count >= 2) {
            stddev = Math.sqrt(sdPartial / (count - 1));
        }
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.stddev = stddev;
        LOG.debug("Stats for {} are {}", values, this);
    }

    /**
     * A Constructor for the pre computed stats.
     * @param mean the mean of the values.
     * @param stddev the standard deviation of the values.
     * @param min the min of the values.
     * @param max the max of the values.
     */
    public NormalDistStats(double mean, double stddev, double min, double max) {
        this.mean = mean;
        this.stddev = stddev;
        this.min = min;
        this.max = max;
    }

    /**
     * Generate a random number that follows the statistical distribution
     * @param rand the random number generator to use
     * @return the next number that should follow the statistical distribution.
     */
    public double nextRandom(Random rand) {
        return Math.max(Math.min((rand.nextGaussian() * stddev) + mean, max), min);
    }

    @Override
    public String toString() {
        return "mean: " + mean + " min: " + min + " max: " + max + " stddev: " + stddev;
    }

    /**
     * Scale the stats by v. This is not scaling everything proportionally.  We don't want the stddev to increase
     * so instead we scale the mean and shift everything up or down by the same amount.
     * @param v the amount to scale by 1.0 is nothing 0.5 is half.
     * @return a copy of this with the needed adjustments.
     */
    public NormalDistStats scaleBy(double v) {
        double newMean = mean * v;
        double shiftAmount = newMean - mean;
        return new NormalDistStats(Math.max(0, mean + shiftAmount), stddev,
            Math.max(0, min + shiftAmount), Math.max(0, max + shiftAmount));
    }
}
