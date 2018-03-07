/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.trident;

import java.util.stream.IntStream;

import org.apache.storm.eventhubs.core.Partition;
import org.apache.storm.eventhubs.core.Partitions;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;

public class Coordinator implements IPartitionedTridentSpout.Coordinator<Partitions>,
		IOpaquePartitionedTridentSpout.Coordinator<Partitions> {
	Partitions partitions;

	public Coordinator(EventHubSpoutConfig spoutConfig) {
		partitions = new Partitions();
		IntStream.range(0, spoutConfig.getPartitionCount())
				.forEach(i -> partitions.addPartition(new Partition(String.valueOf(i))));
	}

	@Override
	public void close() {
	}

	@Override
	public Partitions getPartitionsForBatch() {
		return partitions;
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}
}
