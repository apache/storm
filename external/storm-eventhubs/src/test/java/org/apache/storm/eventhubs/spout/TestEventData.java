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
package org.apache.storm.eventhubs.spout;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.storm.eventhubs.core.EventHubMessage;
import org.junit.Test;

import com.microsoft.azure.eventhubs.EventData;

public class TestEventData {
	@Test
	public void testEventDataComparision() {
		EventData ed1 = EventData.create("blah".getBytes());
		EventData.SystemProperties sysprops1 = new EventData.SystemProperties(2L, Instant.now(), "100", null);
		ed1.setSystemProperties(sysprops1);
		EventHubMessage ehm1 = new EventHubMessage(ed1, "0");

		EventData ed2 = EventData.create("blah".getBytes());
		EventData.SystemProperties sysprops2 = new EventData.SystemProperties(3L, Instant.now(), "150", null);
		ed2.setSystemProperties(sysprops2);
		EventHubMessage ehm2 = new EventHubMessage(ed2, "0");

		assertTrue(ehm2.compareTo(ehm1) > 0);
  	}
}
