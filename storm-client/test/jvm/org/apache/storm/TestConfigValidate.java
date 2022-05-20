/*
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

package org.apache.storm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.Subject;

import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.NimbusBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.ConfigValidation.ImpersonationAclUserEntryValidator;
import org.apache.storm.validation.ConfigValidation.IntegerValidator;
import org.apache.storm.validation.ConfigValidation.KryoRegValidator;
import org.apache.storm.validation.ConfigValidation.ListEntryTypeValidator;
import org.apache.storm.validation.ConfigValidation.ListOfListOfStringValidator;
import org.apache.storm.validation.ConfigValidation.NoDuplicateInListValidator;
import org.apache.storm.validation.ConfigValidation.NotNullValidator;
import org.apache.storm.validation.ConfigValidation.PositiveNumberValidator;
import org.apache.storm.validation.ConfigValidation.PowerOf2Validator;
import org.apache.storm.validation.ConfigValidation.RasConstraintsTypeValidator;
import org.apache.storm.validation.ConfigValidation.StringValidator;
import org.apache.storm.validation.ConfigValidation.UserResourcePoolEntryValidator;
import org.apache.storm.validation.ConfigValidationAnnotations.IsExactlyOneOf;
import org.apache.storm.validation.ConfigValidationAnnotations.IsImplementationOfClass;
import org.apache.storm.validation.ConfigValidationAnnotations.IsListEntryCustom;
import org.apache.storm.validation.ConfigValidationAnnotations.IsListEntryType;
import org.apache.storm.validation.ConfigValidationAnnotations.IsMapEntryCustom;
import org.apache.storm.validation.ConfigValidationAnnotations.IsMapEntryType;
import org.apache.storm.validation.ConfigValidationAnnotations.IsNoDuplicateInList;
import org.apache.storm.validation.ConfigValidationAnnotations.IsString;
import org.apache.storm.validation.ConfigValidationAnnotations.NotNull;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConfigValidate {

    @Test
    public void validPacemakerAuthTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.PACEMAKER_AUTH_METHOD, "NONE");
        ConfigValidation.validateFields(conf);
        conf.put(Config.PACEMAKER_AUTH_METHOD, "DIGEST");
        ConfigValidation.validateFields(conf);
        conf.put(Config.PACEMAKER_AUTH_METHOD, "KERBEROS");
        ConfigValidation.validateFields(conf);
    }

    @Test
    public void invalidPacemakerAuthTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.PACEMAKER_AUTH_METHOD, "invalid");
        assertThrows(IllegalArgumentException.class, () -> ConfigValidation.validateFields(conf));
    }

    @Test
    public void validConfigTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG, 5);
        conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 500);
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, true);

        ConfigValidation.validateFields(conf);
    }

    @Test
    public void invalidConfigTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG, 5);
        conf.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS, 500);
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, "invalid");

        assertThrows(IllegalArgumentException.class, () -> ConfigValidation.validateFields(conf));
    }

    @Test
    public void testValidateTopologyBlobStoreMapWithBlobStore() throws Throwable {
        Map<String, Object> topoConf = new HashMap<>();
        Map<String, Map<String, String>> topologyMap = new HashMap<>();
        topologyMap.put("key1", new HashMap<>());
        topologyMap.put("key2", new HashMap<>());
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topologyMap);
        Subject subject = ReqContext.context().subject();

        BlobStore blobStoreMock = mock(BlobStore.class);
        when(blobStoreMock.getBlobMeta("key1", subject)).thenReturn(null);
        when(blobStoreMock.getBlobMeta("key2", subject)).thenThrow(new KeyNotFoundException());

        assertThrows(InvalidTopologyException.class,
            () -> Utils.validateTopologyBlobStoreMap(topoConf, blobStoreMock));
    }

    @Test
    public void testValidateTopologyBlobStoreMissingKey() throws Throwable {
        Map<String, Object> topoConf = new HashMap<>();
        Map<String, Map<String, String>> topologyMap = new HashMap<>();
        topologyMap.put("key1", new HashMap<>());
        topologyMap.put("key2", new HashMap<>());
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topologyMap);

        NimbusBlobStore nimbusBlobStoreMock = mock(NimbusBlobStore.class);
        when(nimbusBlobStoreMock.getBlobMeta("key1")).thenReturn(null);
        when(nimbusBlobStoreMock.getBlobMeta("key2")).thenThrow(new KeyNotFoundException());

        assertThrows(InvalidTopologyException.class,
            () -> Utils.validateTopologyBlobStoreMap(topoConf, nimbusBlobStoreMock));
    }

    @Test
    public void testValidateTopologyBlobStoreMap() throws InvalidTopologyException, AuthorizationException,
            KeyNotFoundException {
        Map<String, Object> topoConf = new HashMap<>();
        Map<String, Map<String, Object>> topologyMap = new HashMap<>();
        Map<String, Object> blobConf = new HashMap<>();
        blobConf.put("uncompress", false);
        topologyMap.put("key1", blobConf);
        topologyMap.put("key2", blobConf);
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topologyMap);

        NimbusBlobStore nimbusBlobStoreMock = mock(NimbusBlobStore.class);
        when(nimbusBlobStoreMock.getBlobMeta("key1")).thenReturn(null);
        when(nimbusBlobStoreMock.getBlobMeta("key2")).thenReturn(null);

        Utils.validateTopologyBlobStoreMap(topoConf, nimbusBlobStoreMock);
    }

    @Test
    public void testValidateTopologyBlobStoreMapInvalidOption() {
        Map<String, Object> topoConf = new HashMap<>();
        Map<String, Map<String, Object>> topologyMap = new HashMap<>();
        Map<String, Object> blobConf = new HashMap<>();
        blobConf.put("uncompress", "false");
        topologyMap.put("key1", blobConf);
        topologyMap.put("key2", blobConf);
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topologyMap);

        NimbusBlobStore nimbusBlobStoreMock = mock(NimbusBlobStore.class);

        assertThrows(InvalidTopologyException.class,
            () -> Utils.validateTopologyBlobStoreMap(topoConf, nimbusBlobStoreMock));
    }

    @Test
    public void defaultYamlTest() {
        Map<String, Object> conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
    }

    @Test
    public void testTopologyWorkersIsInteger() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_WORKERS, 42);
        ConfigValidation.validateFields(conf);

        conf.put(Config.TOPOLOGY_WORKERS, 3.14159);
        try {
            ConfigValidation.validateFields(conf);
            fail("Expected Exception not Thrown");
        } catch (IllegalArgumentException ignore) {
        }
    }

    @Test
    public void testTopologyStatsSampleRateIsFloat() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.5);
        ConfigValidation.validateFields(conf);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 10);
        ConfigValidation.validateFields(conf);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, Double.MAX_VALUE);
        ConfigValidation.validateFields(conf);
    }

    @Test
    public void testWorkerChildoptsIsStringOrStringList() {
        Map<String, Object> conf = new HashMap<>();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        passCases.add(null);
        passCases.add("some string");
        String[] stuff = { "some", "string", "list" };
        passCases.add(Arrays.asList(stuff));

        failCases.add(42);
        Integer[] wrongStuff = { 1, 2, 3 };
        failCases.add(Arrays.asList(wrongStuff));

        //worker.childopts validates
        for (Object value : passCases) {
            conf.put(Config.WORKER_CHILDOPTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            try {
                conf.put(Config.WORKER_CHILDOPTS, value);
                ConfigValidation.validateFields(conf);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }

        //topology.worker.childopts validates
        conf = new HashMap<>();
        for (Object value : passCases) {
            conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            try {
                conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, value);
                ConfigValidation.validateFields(conf);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testValidity() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put("q", "asasdasd");
        conf.put("aaa", Integer.valueOf("123"));
        conf.put("bbb", Long.valueOf("456"));
        List<Object> testList = new ArrayList<>();
        testList.add(1);
        testList.add(2);
        testList.add(Integer.valueOf("3"));
        testList.add(Long.valueOf("4"));
        testList.add(new Float("3"));
        testList.add(new Double("4"));
        testList.add(ImmutableList.of("asdf", 3));
        conf.put("eee", testList);
        assertTrue(Utils.isValidConf(conf));
    }

    @Test
    public void testNonValidConfigChar() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("q", ImmutableList.of("asdf", 'c'));
        assertFalse(Utils.isValidConf(conf));
    }

    @Test
    public void testNonValidConfigRandomObject() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("q", ImmutableList.of("asdf", new TestConfigValidate()));
        assertFalse(Utils.isValidConf(conf));
    }

    @Test
    public void testKryoRegValidator() {
        KryoRegValidator validator = new KryoRegValidator();

        // fail cases
        Object[] failCases = { ImmutableMap.of("f", "g"), ImmutableList.of(1),
            Collections.singletonList(ImmutableMap.of("a", 1))};
        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }

        // pass cases
        validator.validateField("test", Arrays.asList("a", "b", "c", ImmutableMap.of("d", "e"), ImmutableMap.of("f", "g")));
    }

    @Test
    public void testPowerOf2Validator() {
        PowerOf2Validator validator = new PowerOf2Validator();

        Object[] failCases = { 42.42, 42, -33, 23423423423.0, -32, -1, -0.00001, 0, -0, "Forty-two" };
        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }

        Object[] passCases = { 64, 4294967296.0, 1, null };
        for (Object value : passCases) {
            validator.validateField("test", value);
        }
    }

    @Test
    public void testPositiveNumberValidator() {
        PositiveNumberValidator validator = new PositiveNumberValidator();

        Object[] passCases = { null, 1.0, 0.01, 1, 2147483647, 42 };

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCases = { -1.0, -1, -0.01, 0.0, 0, "43", "string" };

        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }

        Object[] passCasesIncludeZero = { null, 1.0, 0.01, 0, 2147483647, 0.0 };

        for (Object value : passCasesIncludeZero) {
            validator.validateField("test", true, value);
        }

        Object[] failCasesIncludeZero = { -1.0, -1, -0.01, "43", "string" };

        for (Object value : failCasesIncludeZero) {
            try {
                validator.validateField("test", true, value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testIntegerValidator() {
        IntegerValidator validator = new IntegerValidator();

        Object[] passCases = { null, 1000, Integer.MAX_VALUE };

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCases = { 1.34, (long) Integer.MAX_VALUE + 1 };

        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void NoDuplicateInListValidator() {
        NoDuplicateInListValidator validator = new NoDuplicateInListValidator();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        Object[] passCase1 = { 1000, 0, -1000 };
        Object[] passCase2 = { "one", "two", "three" };
        Object[] passCase3 = { false, true };
        Object[] passCase4 = { false, true, 1000, 0, -1000, "one", "two", "three" };
        Object[] passCase5 = { 1000.0, 0.0, -1000.0 };
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));
        passCases.add(Arrays.asList(passCase3));
        passCases.add(Arrays.asList(passCase4));
        passCases.add(Arrays.asList(passCase5));
        passCases.add(null);

        for (Object value : passCases) {
            validator.validateField("test", value);
        }

        Object[] failCase1 = { 1000, 0, 1000 };
        Object[] failCase2 = { "one", "one", "two" };
        Object[] failCase3 = { 5.0, 5.0, 6 };
        failCases.add(Arrays.asList(failCase1));
        failCases.add(Arrays.asList(failCase2));
        failCases.add(Arrays.asList(failCase3));
        for (Object value : failCases) {
            try {
                validator.validateField("test", value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testListEntryTypeValidator() {
        Collection<Object> testCases1 = new LinkedList<>();
        Collection<Object> testCases2 = new LinkedList<>();
        Collection<Object> testCases3 = new LinkedList<>();

        Object[] testCase1 = { "one", "two", "three" };
        Object[] testCase2 = { "three" };
        testCases1.add(Arrays.asList(testCase1));
        testCases1.add(Arrays.asList(testCase2));

        for (Object value : testCases1) {
            ListEntryTypeValidator.validateField("test", String.class, value);
        }

        for (Object value : testCases1) {
            try {
                ListEntryTypeValidator.validateField("test", Number.class, value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }

        Object[] testCase3 = { 1000, 0, 1000 };
        Object[] testCase4 = { 5 };
        Object[] testCase5 = { 5.0, 5.0, 6 };
        testCases2.add(Arrays.asList(testCase3));
        testCases2.add(Arrays.asList(testCase4));
        testCases2.add(Arrays.asList(testCase5));
        for (Object value : testCases2) {
            try {
                ListEntryTypeValidator.validateField("test", String.class, value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
        for (Object value : testCases2) {
            ListEntryTypeValidator.validateField("test", Number.class, value);
        }

        Object[] testCase6 = { 1000, 0, 1000, "5" };
        Object[] testCase7 = { "4", "5", 5 };
        testCases3.add(Arrays.asList(testCase6));
        testCases3.add(Arrays.asList(testCase7));
        for (Object value : testCases3) {
            try {
                ListEntryTypeValidator.validateField("test", String.class, value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
        for (Object value : testCases1) {
            try {
                ListEntryTypeValidator.validateField("test", Number.class, value);
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testMapEntryTypeAnnotation() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();
        Map<Object, Object> passCase1 = new HashMap<>();
        passCase1.put("aaa", 5);
        passCase1.put("bbb", 6);
        passCase1.put("ccc", 7);
        passCases.add(passCase1);
        passCases.add(null);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        Map<Object, Object> failCase1 = new HashMap<>();
        failCase1.put("aaa", 5);
        failCase1.put(5, 6);
        failCase1.put("ccc", 7);
        Map<Object, Object> failCase2 = new HashMap<>();
        failCase2.put("aaa", "str");
        failCase2.put("bbb", 6);
        failCase2.put("ccc", 7);

        failCases.add(failCase1);
        failCases.add(failCase2);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testMapEntryCustomAnnotation() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();
        Map<Object, Object> passCase1 = new HashMap<>();
        passCase1.put("aaa", 5);
        passCase1.put("bbb", 100);
        passCase1.put("ccc", Integer.MAX_VALUE);
        passCases.add(passCase1);
        passCases.add(null);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_2, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        Map<Object, Object> failCase1 = new HashMap<>();
        failCase1.put("aaa", 5);
        failCase1.put(5, 6);
        failCase1.put("ccc", 7);
        Map<Object, Object> failCase2 = new HashMap<>();
        failCase2.put("aaa", "str");
        failCase2.put("bbb", 6);
        failCase2.put("ccc", 7);
        Map<Object, Object> failCase3 = new HashMap<>();
        failCase3.put("aaa", -1);
        failCase3.put("bbb", 6);
        failCase3.put("ccc", 7);
        Map<Object, Object> failCase4 = new HashMap<>();
        failCase4.put("aaa", 1);
        failCase4.put("bbb", 6);
        failCase4.put("ccc", 7.4);

        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add(failCase3);
        failCases.add(failCase4);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_2, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testExactlyOneOfCustomAnnotation() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        List<Object> passCaseListOfList = new ArrayList<>();
        passCaseListOfList.add(Arrays.asList("comp1", "comp2"));
        passCaseListOfList.add(Arrays.asList("comp1", "comp3"));
        passCaseListOfList.add(Arrays.asList("comp2", "comp4"));
        passCaseListOfList.add(Arrays.asList("comp2", "comp5"));

        Map<Object, Object> passCaseMapOfMap = new HashMap<>();
        passCaseMapOfMap.put("comp1",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 10 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp2", "comp3")},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCaseMapOfMap.put("comp2",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 2 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp4", "comp5")},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCases.add(passCaseMapOfMap);

        passCaseMapOfMap = new HashMap<>();
        passCaseMapOfMap.put("comp1",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp2", "comp3")},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCaseMapOfMap.put("comp2",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 2 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp4", "comp5")},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCases.add(passCaseMapOfMap);

        passCaseMapOfMap = new HashMap<>();
        passCaseMapOfMap.put("comp1",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, "comp2"},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCaseMapOfMap.put("comp2",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 2 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, "comp4"},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        passCases.add(passCaseMapOfMap);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_9, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        List<Object> failCaseList = new ArrayList<>();
        failCaseList.add(Arrays.asList("comp1", Arrays.asList("comp2", "comp3")));
        failCaseList.add(Arrays.asList("comp3", Arrays.asList("comp4", "comp5")));
        failCases.add(failCaseList);

        Map<String, Object> failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("comp1",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 10 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList(1, 2, 3)},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        failCaseMapOfMap.put("comp2",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 2 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp4", "comp5")},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("comp1",
                Stream.of(new Object[][] {
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, 10 },
                        { RasConstraintsTypeValidator.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, Arrays.asList("comp1", 3)},
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
        );
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("comp1", Arrays.asList("comp2", "comp3"));
        failCaseMapOfMap.put("comp2", Arrays.asList("comp4", "comp5"));
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("aaa", "str");
        failCaseMapOfMap.put("bbb", 6);
        failCaseMapOfMap.put("ccc", 7);
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("aaa", -1);
        failCaseMapOfMap.put("bbb", 6);
        failCaseMapOfMap.put("ccc", 7);
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("aaa", 1);
        failCaseMapOfMap.put("bbb", 6);
        failCaseMapOfMap.put("ccc", 7.4);
        failCases.add(failCaseMapOfMap);

        failCaseMapOfMap = new HashMap<>();
        failCaseMapOfMap.put("comp1", "comp2");
        failCaseMapOfMap.put("comp2", "comp4");
        failCases.add(failCaseMapOfMap);

        failCases.add(null);

        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_9, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testListEntryTypeAnnotation() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();
        Object[] passCase1 = { 1, 5.0, -0.01, 0, Integer.MAX_VALUE, Double.MIN_VALUE };
        Object[] passCase2 = { 1 };
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_3, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        Object[] failCase1 = { 1, 5.0, -0.01, 0, "aaa" };
        Object[] failCase2 = { "aaa" };
        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add(1);
        failCases.add("b");
        failCases.add(null);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_3, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void testListEntryCustomAnnotation() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();
        Object[] passCase1 = { 1, 5.0, 0.01, Double.MAX_VALUE };
        Object[] passCase2 = { 1 };
        passCases.add(Arrays.asList(passCase1));
        passCases.add(Arrays.asList(passCase2));

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_4, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        Object[] failCase1 = { 1, 5.0, -0.01, 3.0 };
        Object[] failCase2 = { 1, 5.0, -0.01, 1 };
        Object[] failCase3 = { "aaa", "bbb", "aaa" };
        Object[] failCase4 = { 1, 5.0, null, 1 };
        Object[] failCase5 = { 1, 5.0, 0, 1 };

        failCases.add(Arrays.asList(failCase1));
        failCases.add(Arrays.asList(failCase2));
        failCases.add(Arrays.asList(failCase3));
        failCases.add(Arrays.asList(failCase4));
        failCases.add(Arrays.asList(failCase5));
        failCases.add(1);
        failCases.add("b");
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_4, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void TestAcceptedStrings() {
        TestConfig config = new TestConfig();
        String[] passCases = { "aaa", "bbb", "ccc" };

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_5, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        String[] failCases = { "aa", "bb", "cc", "abc", "a", "b", "c", "" };
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_5, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void TestImpersonationAclUserEntryValidator() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        Map<String, Map<String, List<String>>> passCase1 = new HashMap<>();
        Map<String, List<String>> passCase1_hostsAndGroups = new HashMap<>();
        String[] hosts = { "host.1", "host.2", "host.3" };
        passCase1_hostsAndGroups.put("hosts", Arrays.asList(hosts));
        String[] groups = { "group.1", "group.2", "group.3" };
        passCase1_hostsAndGroups.put("groups", Arrays.asList(groups));
        passCase1.put("jerry", passCase1_hostsAndGroups);
        passCases.add(passCase1);

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_6, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }

        Map<String, Map<String, List<String>>> failCase1 = new HashMap<>();
        Map<String, List<String>> failCase1_hostsAndGroups = new HashMap<>();
        failCase1_hostsAndGroups.put("hosts", Arrays.asList(hosts));
        failCase1.put("jerry", failCase1_hostsAndGroups);


        Map<String, Map<String, List<String>>> failCase2 = new HashMap<>();
        Map<String, List<String>> failCase2_hostsAndGroups = new HashMap<>();
        String[] failgroups = { "group.1", "group.2", "group.3" };
        failCase2_hostsAndGroups.put("groups", Arrays.asList(groups)); // should this be failGroups ?
        failCase2.put("jerry", failCase2_hostsAndGroups);

        failCases.add(failCase1);
        failCases.add(failCase2);
        failCases.add("stuff");
        failCases.add(5);

        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_6, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void TestResourceAwareSchedulerUserPool() {
        TestConfig config = new TestConfig();
        Collection<Object> failCases = new LinkedList<>();

        Map<String, Map<String, Integer>> passCase1 = new HashMap<>();
        passCase1.put("jerry", new HashMap<>());
        passCase1.put("bobby", new HashMap<>());
        passCase1.put("derek", new HashMap<>());

        passCase1.get("jerry").put("cpu", 10000);
        passCase1.get("jerry").put("memory", 20148);
        passCase1.get("bobby").put("cpu", 20000);
        passCase1.get("bobby").put("memory", 40148);
        passCase1.get("derek").put("cpu", 30000);
        passCase1.get("derek").put("memory", 60148);

        config.put(TestConfig.TEST_MAP_CONFIG_7, passCase1);
        ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));

        Map<String, Map<String, Integer>> failCase1 = new HashMap<>();
        failCase1.put("jerry", new HashMap<>());
        failCase1.put("bobby", new HashMap<>());
        failCase1.put("derek", new HashMap<>());

        failCase1.get("jerry").put("cpu", 10000);
        failCase1.get("jerry").put("memory", 20148);
        failCase1.get("bobby").put("cpu", 20000);
        failCase1.get("bobby").put("memory", 40148);
        //this will fail the test since user derek does not have an entry for memory
        failCase1.get("derek").put("cpu", 30000);

        Map<String, Map<String, Integer>> failCase2 = new HashMap<>();
        //this will fail since jerry doesn't have either cpu or memory entries
        failCase2.put("jerry", new HashMap<>());
        failCase2.put("bobby", new HashMap<>());
        failCase2.put("derek", new HashMap<>());
        failCase2.get("bobby").put("cpu", 20000);
        failCase2.get("bobby").put("memory", 40148);
        failCase2.get("derek").put("cpu", 30000);
        failCase2.get("derek").put("memory", 60148);

        failCases.add(failCase1);
        failCases.add(failCase2);

        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_7, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    @Test
    public void TestImplementsClassValidator() {
        TestConfig config = new TestConfig();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        passCases.add("org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");

        for (Object value : passCases) {
            config.put(TestConfig.TEST_MAP_CONFIG_8, value);
            ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
        }
        //will fail since org.apache.storm.nimbus.NimbusInfo doesn't implement or extend org.apache.storm.networktopography
        // .DNSToSwitchMapping
        failCases.add("org.apache.storm.nimbus.NimbusInfo");
        failCases.add(null);
        for (Object value : failCases) {
            try {
                config.put(TestConfig.TEST_MAP_CONFIG_8, value);
                ConfigValidation.validateFields(config, Collections.singletonList(TestConfig.class));
                fail("Expected Exception not Thrown for value: " + value);
            } catch (IllegalArgumentException ignore) {
            }
        }
    }

    public static class TestConfig extends HashMap<String, Object> {
        @IsMapEntryType(keyType = String.class, valueType = Integer.class)
        public static final String TEST_MAP_CONFIG = "test.map.config";

        @IsMapEntryCustom(
            keyValidatorClasses = { StringValidator.class },
            valueValidatorClasses = { PositiveNumberValidator.class, IntegerValidator.class })
        public static final String TEST_MAP_CONFIG_2 = "test.map.config.2";

        @IsListEntryType(type = Number.class)
        @NotNull
        public static final String TEST_MAP_CONFIG_3 = "test.map.config.3";

        @IsListEntryCustom(
            entryValidatorClasses = { PositiveNumberValidator.class, NotNullValidator.class })
        @IsNoDuplicateInList
        public static final String TEST_MAP_CONFIG_4 = "test.map.config.4";

        @IsString(acceptedValues = { "aaa", "bbb", "ccc" })
        public static final String TEST_MAP_CONFIG_5 = "test.map.config.5";

        @IsMapEntryCustom(keyValidatorClasses = { StringValidator.class }, valueValidatorClasses = { ImpersonationAclUserEntryValidator
            .class })
        public static final String TEST_MAP_CONFIG_6 = "test.map.config.6";

        @IsMapEntryCustom(keyValidatorClasses = { StringValidator.class }, valueValidatorClasses = { UserResourcePoolEntryValidator.class })
        public static final String TEST_MAP_CONFIG_7 = "test.map.config.7";

        @IsImplementationOfClass(implementsClass = org.apache.storm.networktopography.DNSToSwitchMapping.class)
        @NotNull
        public static final String TEST_MAP_CONFIG_8 = "test.map.config.8";

        @IsExactlyOneOf(valueValidatorClasses = {ListOfListOfStringValidator.class, RasConstraintsTypeValidator.class})
        @NotNull
        public static final String TEST_MAP_CONFIG_9 = "test.map.config.9";
    }
}
