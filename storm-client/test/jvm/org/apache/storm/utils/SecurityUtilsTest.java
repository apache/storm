/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SecurityUtilsTest {

    @Test
    public void testInferKeyStoreTypeFromPath_Pkcs12Extension() {
        Assertions.assertEquals("PKCS12", SecurityUtils.inferKeyStoreTypeFromPath("keystore.p12"));
        Assertions.assertEquals("PKCS12", SecurityUtils.inferKeyStoreTypeFromPath("mykeys.P12"));
        Assertions.assertEquals("PKCS12", SecurityUtils.inferKeyStoreTypeFromPath("path/to/keystore.pkcs12"));
        Assertions.assertEquals("PKCS12", SecurityUtils.inferKeyStoreTypeFromPath("mykeys.pKCS12"));
        Assertions.assertEquals("PKCS12", SecurityUtils.inferKeyStoreTypeFromPath("another/path/to/keystore.pfx"));
    }

    @Test
    public void testInferKeyStoreTypeFromPath_JksExtension() {
        Assertions.assertEquals("JKS", SecurityUtils.inferKeyStoreTypeFromPath("keystore.jks"));
        Assertions.assertEquals("JKS", SecurityUtils.inferKeyStoreTypeFromPath("mykeys.JKS"));
        Assertions.assertEquals("JKS", SecurityUtils.inferKeyStoreTypeFromPath("path/to/keystore.jKs"));
    }

    @Test
    public void testInferKeyStoreTypeFromPath_UnsupportedExtension() {
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath("keystore.pem"));
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath("certificate.crt"));
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath("path/to/keystore.txt"));
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath("another/path/to/keystore.pem"));
    }

    @Test
    public void testInferKeyStoreTypeFromPath_EmptyOrNullPath() {
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath(""));
        Assertions.assertNull(SecurityUtils.inferKeyStoreTypeFromPath(null));
    }
}
