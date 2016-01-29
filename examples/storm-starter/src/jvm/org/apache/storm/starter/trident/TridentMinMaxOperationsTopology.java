/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.NumberGeneratorSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


public class TridentMinMaxOperationsTopology {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildIdsTopology() {
        NumberGeneratorSpout spout = new NumberGeneratorSpout(new Fields("id"), 10, 1000);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("numgen-spout", spout).
                each(new Fields("id"), new Debug("##### ids"));

        wordsStream.min("id", "min-id").
                each(new Fields("min-id"), new Debug("#### min-id"));

        wordsStream.max("id", "max-id").
                each(new Fields("max-id"), new Debug("#### max-id"));

        return topology.build();
    }

    public static StormTopology buildWordsTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("spout1", spout).parallelismHint(16).
                each(new Fields("sentence"), new Split(), new Fields("word")).
                each(new Fields("word"), new Debug("##### words"));

        wordsStream.min("word", "lowest").
                each(new Fields("lowest"), new Debug("#### lowest word"));

        wordsStream.max("word", "highest").
                each(new Fields("highest"), new Debug("#### highest word"));

        return topology.build();
    }

    public static StormTopology buildVehiclesTopology() {

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("vehicles"), 10, Vehicle.generateVehicles(20));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream wordsStream = topology.newStream("spout1", spout).
                each(new Fields("vehicles"), new Debug("##### vehicles"));

        wordsStream.min("vehicles", "slowest", new SpeedComparator()).
                each(new Fields("slowest"), new Debug("#### slowest vehicle"));

        wordsStream.max("vehicles", "fastest", new SpeedComparator()).
                each(new Fields("fastest"), new Debug("#### fastest vehicle"));

        wordsStream.max("vehicles", "efficient", new EfficiencyComparator()).
                each(new Fields("efficient"), new Debug("#### efficient vehicle"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        StormTopology[] topologies = {buildWordsTopology(), buildIdsTopology(), buildVehiclesTopology()};
        if (args.length == 0) {
            for (StormTopology topology : topologies) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("min-max-topology", conf, topology);
                Utils.sleep(60*1000);
                cluster.shutdown();
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            int ct=1;
            for (StormTopology topology : topologies) {
                StormSubmitter.submitTopologyWithProgressBar(args[0]+"-"+ct++, conf, topology);
            }
        }
    }

    static class SpeedComparator implements Comparator<Vehicle>, Serializable {
        @Override
        public int compare(Vehicle vehicle1, Vehicle vehicle2) {
            return Integer.compare(vehicle1.maxSpeed, vehicle2.maxSpeed);
        }
    }

    static class EfficiencyComparator implements Comparator<Vehicle>, Serializable {
        @Override
        public int compare(Vehicle vehicle1, Vehicle vehicle2) {
            return Double.compare(vehicle1.efficiency, vehicle2.efficiency);
        }
    }

    static class Vehicle implements Serializable {
        final String name;
        final int maxSpeed;
        final double efficiency;

        public Vehicle(String name, int maxSpeed, double efficiency) {
            this.name = name;
            this.maxSpeed = maxSpeed;
            this.efficiency = efficiency;
        }

        @Override
        public String toString() {
            return "Vehicle{" +
                    "name='" + name + '\'' +
                    ", maxSpeed=" + maxSpeed +
                    ", efficiency=" + efficiency +
                    '}';
        }

        public static List<Object>[] generateVehicles(int count) {
            List<Object>[] vehicles = new List[count];
            for(int i=0; i<count; i++) {
                vehicles[i] = (new Values(new Vehicle("Vehicle-"+(i+1), ThreadLocalRandom.current().nextInt(0, 100), ThreadLocalRandom.current().nextDouble(1, 5))));
            }
            return vehicles;
        }
    }
}
