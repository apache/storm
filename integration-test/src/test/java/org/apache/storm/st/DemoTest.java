/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.storm.st.helper.AbstractTest;
import org.apache.storm.st.wrapper.TopoWrap;
import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.TopologyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.apache.storm.st.utils.TimeUtil;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public final class DemoTest extends AbstractTest {
    private static Logger log = LoggerFactory.getLogger(DemoTest.class);
    private static Collection<String> words = Lists.newArrayList("nathan", "mike", "jackson", "golda", "bertels");
    private static Collection<String> exclaim2Oputput = Collections2.transform(words, new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            return input +  "!!!!!!";
        }
    });
    protected final String topologyName = this.getClass().getSimpleName();
    private TopoWrap topo;

    @Test
    public void testExclamationTopology() throws Exception {
        topo = new TopoWrap(cluster, topologyName, ExclamationTopology.getStormTopology());
        topo.submitSuccessfully();
        final int minExclaim2Emits = 500;
        final int minSpountEmits = 10000;
        for(int i = 0; i < 10; ++i) {
            TopologyInfo topologyInfo = topo.getInfo();
            log.info(topologyInfo.toString());
            long wordSpoutEmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.WORD);
            long exclaim1EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_1);
            long exclaim2EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_2);
            log.info("wordSpoutEmittedCount for spout 'word' = " + wordSpoutEmittedCount);
            log.info("exclaim1EmittedCount = " + exclaim1EmittedCount);
            log.info("exclaim2EmittedCount = " + exclaim2EmittedCount);
            if (exclaim2EmittedCount > minExclaim2Emits || wordSpoutEmittedCount > minSpountEmits) {
                break;
            }
            TimeUtil.sleepSec(6);
        }
        List<TopoWrap.ExecutorURL> boltUrls = topo.getLogUrls(ExclamationTopology.WORD);
        log.info(boltUrls.toString());
        final String actualOutput = topo.getLogs(ExclamationTopology.EXCLAIM_2);
        for (String oneExpectedOutput : exclaim2Oputput) {
            Assert.assertTrue(actualOutput.contains(oneExpectedOutput), "Couldn't find " + oneExpectedOutput + " in urls");
        }
    }

    @AfterMethod
    public void cleanup() throws Exception {
        if (topo != null) {
            topo.killQuietly();
        }
    }
}
