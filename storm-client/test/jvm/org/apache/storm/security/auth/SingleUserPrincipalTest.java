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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SingleUserPrincipalTest {

    @Test
    public void testEqualsAndHashCode() {
        assertEquals(new SingleUserPrincipal("alice"), new SingleUserPrincipal("alice"));
        assertEquals(new SingleUserPrincipal("alice").hashCode(), new SingleUserPrincipal("alice").hashCode());
        assertNotEquals(new SingleUserPrincipal("alice"), new SingleUserPrincipal("bob"));
        assertNotEquals(new SingleUserPrincipal("alice"), null);
    }

    @Test
    public void testNullName() {
        SingleUserPrincipal noName = new SingleUserPrincipal(null);
        assertNull(noName.getName());
        assertEquals(noName, new SingleUserPrincipal(null));
        assertEquals(noName.hashCode(), new SingleUserPrincipal(null).hashCode());
        assertNotEquals(noName, new SingleUserPrincipal("alice"));
        assertNotEquals(new SingleUserPrincipal("alice"), noName);
    }
}
