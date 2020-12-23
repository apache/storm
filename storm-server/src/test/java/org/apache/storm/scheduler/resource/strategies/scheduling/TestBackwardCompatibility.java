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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.storm.TestRebalance;
import org.apache.storm.daemon.nimbus.NimbusTest;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.scheduler.blacklist.TestBlacklistScheduler;
import org.apache.storm.scheduler.resource.TestResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUser;
import org.apache.storm.scheduler.resource.strategies.eviction.TestDefaultEvictionStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.TestFIFOSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.TestGenericResourceAwareSchedulingPriorityStrategy;
import org.apache.storm.testing.PerformanceTest;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test for backward compatibility.
 *
 * <p>
 * {@link GenericResourceAwareStrategyOld} class behavior is supposed to be compatible
 * with the prior version of {@link GenericResourceAwareStrategy} and
 * {@link DefaultResourceAwareStrategyOld} class behavior is supposed to be compatible
 * with the prior version of {@link DefaultResourceAwareStrategy}.
 * </p>
 *
 * The tests in this class wrap tests in other classes while replacing Strategy classes.
 * The wrapped classes have protected methods that return strategy classes. These methods
 * are overridden to return backward compatible class.
 */
public class TestBackwardCompatibility {

    TestGenericResourceAwareStrategy testGenericResourceAwareStrategy;
    TestResourceAwareScheduler testResourceAwareScheduler;
    TestBlacklistScheduler testBlacklistScheduler;
    NimbusTest nimbusTest;
    TestRebalance testRebalance;
    TestGenericResourceAwareSchedulingPriorityStrategy testGenericResourceAwareSchedulingPriorityStrategy;

    TestDefaultResourceAwareStrategy testDefaultResourceAwareStrategy;
    TestFIFOSchedulingPriorityStrategy testFIFOSchedulingPriorityStrategy;
    TestDefaultEvictionStrategy testDefaultEvictionStrategy;
    TestUser testUser;

    public TestBackwardCompatibility() {
        // Create instances of wrapped test classes and override strategy class methods
        testGenericResourceAwareStrategy = new TestGenericResourceAwareStrategy() {
            @Override
            protected Class getGenericResourceAwareStrategyClass() {
                return GenericResourceAwareStrategyOld.class;
            }
        };
        testResourceAwareScheduler = new TestResourceAwareScheduler() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }

            @Override
            protected Class getGenericResourceAwareStrategyClass() {
                return GenericResourceAwareStrategyOld.class;
            }
        };
        testBlacklistScheduler = new TestBlacklistScheduler() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        nimbusTest = new NimbusTest() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        testRebalance = new TestRebalance() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        testGenericResourceAwareSchedulingPriorityStrategy = new TestGenericResourceAwareSchedulingPriorityStrategy() {
            @Override
            protected Class getGenericResourceAwareStrategyClass() {
                return GenericResourceAwareStrategyOld.class;
            }
        };
        testDefaultResourceAwareStrategy = new TestDefaultResourceAwareStrategy() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        testFIFOSchedulingPriorityStrategy = new TestFIFOSchedulingPriorityStrategy() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        testDefaultEvictionStrategy = new TestDefaultEvictionStrategy() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };
        testUser = new TestUser() {
            @Override
            protected Class getDefaultResourceAwareStrategyClass() {
                return DefaultResourceAwareStrategyOld.class;
            }
        };

    }

    /**********************************************************************************
     *  Tests for  testGenericResourceAwareStrategy
     ***********************************************************************************/

    @Test
    public void testGenericResourceAwareStrategySharedMemory() {
        testGenericResourceAwareStrategy.testGenericResourceAwareStrategySharedMemory();
    }

    @Test
    public void testGenericResourceAwareStrategy()
        throws InvalidTopologyException {
        testGenericResourceAwareStrategy.testGenericResourceAwareStrategyWithoutSettingAckerExecutors(0);
    }

    @Test
    public void testGenericResourceAwareStrategyInFavorOfShuffle()
        throws InvalidTopologyException {
        testGenericResourceAwareStrategy.testGenericResourceAwareStrategyInFavorOfShuffle();
    }

    @Test
    public void testGrasRequiringEviction() {
        testGenericResourceAwareStrategy.testGrasRequiringEviction();
    }

    @Test
    public void testAntiAffinityWithMultipleTopologies() {
        testGenericResourceAwareStrategy.testAntiAffinityWithMultipleTopologies();
    }

    /**********************************************************************************
     *  Tests for  testResourceAwareScheduler
     ***********************************************************************************/

    @PerformanceTest
    @Test
    public void testLargeTopologiesOnLargeClusters() {
        testResourceAwareScheduler.testLargeTopologiesOnLargeClusters();
    }

    @PerformanceTest
    @Test
    public void testLargeTopologiesOnLargeClustersGras() {
        testResourceAwareScheduler.testLargeTopologiesOnLargeClustersGras();
    }

    @Test
    public void testHeterogeneousClusterwithGras() {
        testResourceAwareScheduler.testHeterogeneousClusterwithGras();
    }

    @Test
    public void testRASNodeSlotAssign() {
        testResourceAwareScheduler.testRASNodeSlotAssign();
    }

    @Test
    public void sanityTestOfScheduling() {
        testResourceAwareScheduler.sanityTestOfScheduling();
    }

    @Test
    public void testTopologyWithMultipleSpouts() {
        testResourceAwareScheduler.testTopologyWithMultipleSpouts();
    }

    @Test
    public void testTopologySetCpuAndMemLoad() {
        testResourceAwareScheduler.testTopologySetCpuAndMemLoad();
    }

    @Test
    public void testResourceLimitation() {
        testResourceAwareScheduler.testResourceLimitation();
    }

    @Test
    public void testScheduleResilience() {
        testResourceAwareScheduler.testScheduleResilience();
    }

    @Test
    public void testHeterogeneousClusterwithDefaultRas() {
        testResourceAwareScheduler.testHeterogeneousClusterwithDefaultRas();
    }

    @Test
    public void testTopologyWorkerMaxHeapSize() {
        testResourceAwareScheduler.testTopologyWorkerMaxHeapSize();
    }

    @Test
    public void testReadInResourceAwareSchedulerUserPools() {
        testResourceAwareScheduler.testReadInResourceAwareSchedulerUserPools();
    }

    @Test
    public void testSubmitUsersWithNoGuarantees() {
        testResourceAwareScheduler.testSubmitUsersWithNoGuarantees();
    }

    @Test
    public void testMultipleUsers() {
        testResourceAwareScheduler.testMultipleUsers();
    }

    @Test
    public void testHandlingClusterSubscription() {
        testResourceAwareScheduler.testHandlingClusterSubscription();
    }

    @Test
    public void testFaultTolerance() {
        testResourceAwareScheduler.testFaultTolerance();
    }

    @Test
    public void testNodeFreeSlot() {
        testResourceAwareScheduler.testNodeFreeSlot();
    }

    @Test
    public void testSchedulingAfterFailedScheduling() {
        testResourceAwareScheduler.testSchedulingAfterFailedScheduling();
    }

    @Test
    public void minCpuWorkerJustFits() {
        testResourceAwareScheduler.minCpuWorkerJustFits();
    }

    @Test
    public void minCpuPreventsThirdTopo() {
        testResourceAwareScheduler.minCpuPreventsThirdTopo();
    }

    @Test
    public void testMinCpuMaxMultipleSupervisors() {
        testResourceAwareScheduler.testMinCpuMaxMultipleSupervisors();
    }

    @Test
    public void minCpuWorkerSplitFails() {
        testResourceAwareScheduler.minCpuWorkerSplitFails();
    }

    @Test
    public void TestLargeFragmentedClusterScheduling() {
        testResourceAwareScheduler.TestLargeFragmentedClusterScheduling();
    }

    @Test
    public void testMultipleSpoutsAndCyclicTopologies() {
        testResourceAwareScheduler.testMultipleSpoutsAndCyclicTopologies();
    }

    @Test
    public void testSchedulerStrategyWhitelist() {
        testResourceAwareScheduler.testSchedulerStrategyWhitelist();
    }

    @Test
    public void testSchedulerStrategyWhitelistException() {
        testResourceAwareScheduler.testSchedulerStrategyWhitelistException();
    }

    @Test
    public void testSchedulerStrategyEmptyWhitelist() {
        testResourceAwareScheduler.testSchedulerStrategyEmptyWhitelist();
    }

    @Test
    public void testStrategyTakingTooLong() {
        testResourceAwareScheduler.testStrategyTakingTooLong();
    }

    /**********************************************************************************
     *  Tests for  TestBlackListScheduler
     ***********************************************************************************/
    @Test
    public void TestGreylist() {
        testBlacklistScheduler.TestGreylist();
    }

    /**********************************************************************************
     *  Tests for  NimbusTest
     ***********************************************************************************/
    @Test
    public void testMemoryLoadLargerThanMaxHeapSize() throws Exception {
        nimbusTest.testMemoryLoadLargerThanMaxHeapSize();
    }

    /**********************************************************************************
     *  Tests for  TestRebalance
     ***********************************************************************************/
    @Test
    public void testRebalanceTopologyResourcesAndConfigs() throws Exception {
        testRebalance.testRebalanceTopologyResourcesAndConfigs();
    }

    /**********************************************************************************
     *  Tests for  testGenericResourceAwareSchedulingPriorityStrategy
     ***********************************************************************************/
    @Test
    public void testDefaultSchedulingPriorityStrategyNotEvicting() {
        testGenericResourceAwareSchedulingPriorityStrategy.testDefaultSchedulingPriorityStrategyNotEvicting();
    }

    @Test
    public void testDefaultSchedulingPriorityStrategyEvicting() {
        testGenericResourceAwareSchedulingPriorityStrategy.testDefaultSchedulingPriorityStrategyEvicting();
    }

    @Test
    public void testGenericSchedulingPriorityStrategyEvicting() {
        testGenericResourceAwareSchedulingPriorityStrategy.testGenericSchedulingPriorityStrategyEvicting();
    }

    /**********************************************************************************
     *  Tests for  testDefaultResourceAwareStrategy
     ***********************************************************************************/

    @Test
    public void testSchedulingNegativeResources() {
        testDefaultResourceAwareStrategy.testSchedulingNegativeResources();
    }

    @ParameterizedTest
    @EnumSource(TestDefaultResourceAwareStrategy.WorkerRestrictionType.class)
    public void testDefaultResourceAwareStrategySharedMemory(TestDefaultResourceAwareStrategy.WorkerRestrictionType schedulingLimitation) {
        testDefaultResourceAwareStrategy.testDefaultResourceAwareStrategySharedMemory(schedulingLimitation);
    }

    @Test
    public void testDefaultResourceAwareStrategy()
        throws InvalidTopologyException {
        testDefaultResourceAwareStrategy.testDefaultResourceAwareStrategyWithoutSettingAckerExecutors(0);
    }

    @Test
    public void testDefaultResourceAwareStrategyInFavorOfShuffle()
        throws InvalidTopologyException {
        testDefaultResourceAwareStrategy.testDefaultResourceAwareStrategyInFavorOfShuffle();
    }

    @Test
    public void testMultipleRacks() {
        testDefaultResourceAwareStrategy.testMultipleRacks();
    }

    @Test
    public void testMultipleRacksWithFavoritism() {
        testDefaultResourceAwareStrategy.testMultipleRacksWithFavoritism();
    }

    /**********************************************************************************
     *  Tests for  TestFIFOSchedulingPriorityStrategy
     ***********************************************************************************/

    @Test
    public void testFIFOEvictionStrategy() {
        testFIFOSchedulingPriorityStrategy.testFIFOEvictionStrategy();
    }

    /**********************************************************************************
     *  Tests for  TestDefaultEvictionStrategy
     ***********************************************************************************/

    @Test
    public void testEviction() {
        testDefaultEvictionStrategy.testEviction();
    }

    @Test
    public void testEvictMultipleTopologies() {
        testDefaultEvictionStrategy.testEvictMultipleTopologies();
    }

    @Test
    public void testEvictMultipleTopologiesFromMultipleUsersInCorrectOrder() {
        testDefaultEvictionStrategy.testEvictMultipleTopologiesFromMultipleUsersInCorrectOrder();
    }

    @Test
    public void testEvictTopologyFromItself() {
        testDefaultEvictionStrategy.testEvictTopologyFromItself();
    }

    @Test
    public void testOverGuaranteeEviction() {
        testDefaultEvictionStrategy.testOverGuaranteeEviction();
    }

    /**********************************************************************************
     *  Tests for  TestUser
     ***********************************************************************************/

    @Test
    public void testResourcePoolUtilization() {
        testUser.testResourcePoolUtilization();
    }
}