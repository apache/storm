/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth;

import org.apache.storm.security.auth.sasl.SaslTransportPlugin;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SaslTransportPluginTest {

    @Test
    public void testUserName() {
        String name = "Andy";
        SaslTransportPlugin.User user = new SaslTransportPlugin.User(name);
        assertEquals(name, user.toString());
        assertEquals(user.getName(), user.toString());
        assertEquals(name.hashCode(), user.hashCode());
    }

    @Test
    public void testUserEquals() {
        String name = "Andy";
        SaslTransportPlugin.User user1 = new SaslTransportPlugin.User(name);
        SaslTransportPlugin.User user2 = new SaslTransportPlugin.User(name);
        SaslTransportPlugin.User user3 = new SaslTransportPlugin.User("Bobby");
        assertTrue(user1.equals(user1));
        assertTrue(user1.equals(user2));
        assertFalse(user1.equals(null));
        assertFalse(user1.equals("Potato"));
        assertFalse(user1.equals(user3));
    }
}
