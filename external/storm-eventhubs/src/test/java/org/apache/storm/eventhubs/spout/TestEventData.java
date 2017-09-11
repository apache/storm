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

import static org.junit.Assert.assertTrue;

import org.apache.storm.eventhubs.core.EventHubMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEventData {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEventDataComparision() {
		EventHubMessage ehm = new EventHubMessage(EventHubReceiverMock.getEventData("mesg1", "3", 1L, null), "1");
		EventHubMessage ehm1 = new EventHubMessage(EventHubReceiverMock.getEventData("mesg1", "13", 2L, null), "2");
				
		assertTrue(ehm1.compareTo(ehm) > 0);
	}
}
