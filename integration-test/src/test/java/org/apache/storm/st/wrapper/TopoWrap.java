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

package org.apache.storm.st.wrapper;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.st.topology.window.data.FromJson;
import org.apache.storm.st.utils.AssertUtil;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.st.utils.TimeUtil;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class TopoWrap {
    private static final Logger LOG = LoggerFactory.getLogger(TopoWrap.class);
    private static final Map<String, Object> SUBMIT_CONF = getSubmitConf();
    private final StormCluster cluster;
    private final String name;
    private final StormTopology topology;
    private String id;
    static {
        String jarFile = getJarPath();
        LOG.info("setting storm.jar to: " + jarFile);
        System.setProperty("storm.jar", jarFile);
    }

    public TopoWrap(StormCluster cluster, String name, StormTopology topology) {
        this.cluster = cluster;
        this.name = name;
        this.topology = topology;
    }

    public void submit(ImmutableMap<String, Object> topoConf) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        final HashMap<String, Object> newConfig = new HashMap<>(SUBMIT_CONF);
        newConfig.putAll(topoConf);
        StormSubmitter.submitTopologyWithProgressBar(name, newConfig, topology);
    }

    private static Map<String, Object> getSubmitConf() {
        Map<String, Object> submitConf = new HashMap<>();
        submitConf.put("storm.zookeeper.topology.auth.scheme", "digest");
        submitConf.put("topology.workers", 3);
        submitConf.put("topology.debug", true);
        //Set the metrics sample rate to 1 to force update the executor stats every time something happens
        //This is necessary because getAllTimeEmittedCount relies on the executor emit stats to be accurate
        submitConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1);
        return submitConf;
    }

    private static String getJarPath() {
        final String USER_DIR = "user.dir";
        String userDirVal = System.getProperty(USER_DIR);
        Assert.assertNotNull(userDirVal, "property " + USER_DIR + " was not set.");
        File projectDir = new File(userDirVal);
        AssertUtil.exists(projectDir);
        
        Collection<File> allJars = FileUtils.listFiles(projectDir, new String[]{"jar"}, true);
        final List<File> jarsExcludingSurefire = allJars.stream()
            .filter(file -> file != null && !file.getName().contains("surefirebooter"))
            .collect(Collectors.toList());
        LOG.info("Found jar files: " + jarsExcludingSurefire);
        AssertUtil.nonEmpty(jarsExcludingSurefire, "The jar file is missing - did you run 'mvn clean package -DskipTests' before running tests ?");
        
        String jarFile = null;
        for (File jarPath : jarsExcludingSurefire) {
            LOG.info("jarPath = " + jarPath);
            if (jarPath != null && !jarPath.getPath().contains("original")) {
                AssertUtil.exists(jarPath);
                jarFile = jarPath.getAbsolutePath();
                break;
            }
        }
        Assert.assertNotNull(jarFile, "Couldn't detect a suitable jar file for uploading.");
        LOG.info("jarFile = " + jarFile);
        return jarFile;
    }

    public void submitSuccessfully(ImmutableMap<String, Object> topoConf) throws TException {
        submit(topoConf);
        TopologySummary topologySummary = getSummary();
        Assert.assertEquals(topologySummary.get_status().toLowerCase(), "active", "Topology must be active.");
        id = topologySummary.get_id();
    }

    public void submitSuccessfully() throws TException {
        submitSuccessfully(ImmutableMap.of());
    }

    private TopologySummary getSummary() throws TException {
        List<TopologySummary> oneTopo = cluster.getSummaries().stream()
            .filter(summary -> summary != null && summary.get_name().equals(name))
            .collect(Collectors.toList());
        AssertUtil.assertOneElement(oneTopo);
        return oneTopo.get(0);
    }

    public TopologyInfo getInfo() throws TException {
        return cluster.getNimbusClient().getTopologyInfo(id);
    }

    public long getComponentExecutorCount(final String componentId) throws TException {
        TopologyInfo info = getInfo();
        List<ExecutorSummary> executors = info.get_executors();
        return executors.stream()
            .filter(summary -> summary != null && summary.get_component_id().equals(componentId))
            .count();
    }
    
    public long getAllTimeEmittedCount(final String componentId) throws TException {
        TopologyInfo info = getInfo();
        final List<ExecutorSummary> executors = info.get_executors();
        
        return executors.stream()
            .filter(summary -> summary != null && summary.get_component_id().equals(componentId))
            .mapToLong(summary -> {
                ExecutorStats executorStats = summary.get_stats();
                if (executorStats == null) {
                    return 0L;
                }                
                Map<String, Map<String, Long>> emitted = executorStats.get_emitted();
                if (emitted == null) {
                    return 0L;
                }
                Map<String, Long> allTime = emitted.get(":all-time");
                if (allTime == null) {
                    return 0L;
                }
                return allTime.get(Utils.DEFAULT_STREAM_ID);
            }).sum();
    }
    
    /**
     * Get the Logviewer worker log URLs for the specified component.
     */
    public Set<ExecutorURL> getLogUrls(final String componentId) throws TException, MalformedURLException {
        ComponentPageInfo componentPageInfo = cluster.getNimbusClient().getComponentPageInfo(id, componentId, null, false);
        List<ExecutorAggregateStats> executorStats = componentPageInfo.get_exec_stats();
        Set<ExecutorURL> urls = new HashSet<>();
        for (ExecutorAggregateStats execStat : executorStats) {
            ExecutorSummary execSummary = execStat.get_exec_summary();
            String host = execSummary.get_host();
            int executorPort = execSummary.get_port();
            //http://supervisor2:8000/download/DemoTest-26-1462229009%2F6703%2Fworker.log
            //http://supervisor2:8000/log?file=SlidingWindowCountTest-9-1462388349%2F6703%2Fworker.log
            int logViewerPort = 8000;
            ExecutorURL executorURL = new ExecutorURL(componentId, host, logViewerPort, executorPort, id);
            urls.add(executorURL);
        }
        return urls;
    }

    public void waitForProgress(int minEmits, int expectedExecutors, String componentName, int maxWaitSec) throws TException {
        for (int i = 0; i < (maxWaitSec + 1) / 2; ++i) {
            LOG.info(getInfo().toString());
            long emitCount = getAllTimeEmittedCount(componentName);
            LOG.info("Count for component " + componentName + " is " + emitCount);
            long executorCount = getComponentExecutorCount(componentName);
            LOG.info("Component " + componentName + " has " + executorCount + " started executors");
            if (emitCount >= minEmits && executorCount == expectedExecutors) {
                break;
            }
            TimeUtil.sleepSec(2);
        }
    }

    public void assertProgress(int minEmits, int expectedExecutors, String componentName, int maxWaitSec) throws TException {
        waitForProgress(minEmits, expectedExecutors, componentName, maxWaitSec);
        long emitCount = getAllTimeEmittedCount(componentName);
        Assert.assertTrue(emitCount >= minEmits, "Emit count for component '" + componentName + "' is " + emitCount + ", min is " + minEmits);
        long executorCount = getComponentExecutorCount(componentName);
        assertThat(executorCount, is((long)expectedExecutors));
    }

    public static class ExecutorURL {
        private final String componentId;
        private final URL viewUrl;
        private final URL downloadUrl;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ExecutorURL)) return false;

            ExecutorURL that = (ExecutorURL) o;

            if (componentId != null ? !componentId.equals(that.componentId) : that.componentId != null) return false;
            if (getViewUrl() != null ? !getViewUrl().equals(that.getViewUrl()) : that.getViewUrl() != null)
                return false;
            return getDownloadUrl() != null ? getDownloadUrl().equals(that.getDownloadUrl()) : that.getDownloadUrl() == null;

        }

        @Override
        public int hashCode() {
            int result = componentId != null ? componentId.hashCode() : 0;
            result = 31 * result + (getViewUrl() != null ? getViewUrl().hashCode() : 0);
            result = 31 * result + (getDownloadUrl() != null ? getDownloadUrl().hashCode() : 0);
            return result;
        }

        public ExecutorURL(String componentId, String host, int logViewerPort, int executorPort, String topoId) throws MalformedURLException {
            String viewUrlStr = String.format("http://%s:%s/api/v1/log?file=", host, logViewerPort);
            String downloadUrlStr = String.format("http://%s:%s/api/v1/download?file=", host, logViewerPort);
            try {
                String workerLogQueryParam = URLEncoder.encode(topoId + "/" + executorPort + "/worker.log", StandardCharsets.UTF_8.name());
                viewUrl = new URL(viewUrlStr + workerLogQueryParam);
                downloadUrl = new URL(downloadUrlStr + workerLogQueryParam);
            } catch (UnsupportedEncodingException e) {
                throw Utils.wrapInRuntime(e);
            }
            this.componentId = componentId;
        }

        public URL getDownloadUrl() {
            return downloadUrl;
        }

        public URL getViewUrl() {
            return viewUrl;
        }

        @Override
        public String toString() {
            return "ExecutorURL{" +
                    "componentId='" + componentId + '\'' +
                    ", viewUrl=" + viewUrl +
                    ", downloadUrl=" + downloadUrl +
                    '}';
        }
    }

    /**
     * Get the log lines that contain the unique {@link StringDecorator} string, deserialized from json.
     * The intent is that test bolts or spouts can write the unique string, followed by json data to log via {@link StringDecorator}.
     * This method will recognize such lines, and deserialize the json data using the provided decoder.
     */
    public <T> List<T> getDeserializedDecoratedLogLines(final String componentId, final FromJson<T> jsonDeserializer) 
            throws IOException, TException, MalformedURLException {
        final List<DecoratedLogLine> logData = getDecoratedLogLines(componentId);
        return deserializeLogData(logData, jsonDeserializer);
    }
    
    public <T> List<T> deserializeLogData(List<DecoratedLogLine> logData, final FromJson<T> jsonDeserializer) {
        return logData.stream()
            .peek(Assert::assertNotNull)
            .map(DecoratedLogLine::getData)
            .map(jsonDeserializer::fromJson)
            .collect(Collectors.toList());
    }

    /**
     * Get the log lines that contain the unique {@link StringDecorator} string for the given component.
     * Test spouts and bolts can write logs containing the StringDecorator string, which can be fetched using this method.
     */
    public List<DecoratedLogLine> getDecoratedLogLines(final String componentId) throws IOException, TException, MalformedURLException {
        final String logs = getLogs(componentId);
        final String dateRegex = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}";
        Pattern pattern = Pattern.compile("(?=\\n" + dateRegex + ")");
        final String[] logLines = pattern.split(logs);
        List<DecoratedLogLine> sortedLogs = Arrays.asList(logLines).stream()
            .filter(log -> log != null && StringDecorator.isDecorated(componentId, log))
            .map(DecoratedLogLine::new)
            .sorted()
            .collect(Collectors.toList());
        LOG.info("Found " + sortedLogs.size() + " items for component: " + componentId);
        return sortedLogs;
    }

    /**
     * Gets all logs for the specified component, concatenated to a single string.
     */
    public String getLogs(final String componentId) throws IOException, TException, MalformedURLException {
        LOG.info("Fetching logs for componentId = " + componentId);
        Set<ExecutorURL> componentLogUrls = getLogUrls(componentId);
        LOG.info("Found " + componentLogUrls.size() + " urls: " + componentLogUrls.toString());
        List<String> urlContents = new ArrayList<>();
        for(ExecutorURL executorUrl : componentLogUrls) {
            if(executorUrl == null || executorUrl.getDownloadUrl() == null) {
                continue;
            }
            LOG.info("Fetching: " + executorUrl);
            URL downloadUrl = executorUrl.downloadUrl;
            String urlContent = IOUtils.toString(downloadUrl, StandardCharsets.UTF_8);
            urlContents.add(urlContent);
            if (urlContent.length() < 500) {
                LOG.info("Fetched: " + urlContent);
            } else {
                LOG.info("Fetched: " + NumberFormat.getNumberInstance(Locale.US).format(urlContent.length()) + " bytes.");
            }
            if (System.getProperty("regression.downloadWorkerLogs").equalsIgnoreCase("true")) {
                downloadLogUrl(downloadUrl, urlContent);
            }
        }
        return StringUtils.join(urlContents, '\n');
    }
    
    private void downloadLogUrl(URL downloadUrl, String urlContent) {
        final String userDir = System.getProperty("user.dir");
        final File target = new File(userDir, "target");
        final File logDir = new File(target, "logs");
        final File logFile = new File(logDir, downloadUrl.getHost() + "-" + downloadUrl.getFile().split("/")[2]);
        try {
            FileUtils.forceMkdir(logDir);
            FileUtils.write(logFile, urlContent, StandardCharsets.UTF_8);
        } catch (Throwable throwable) {
            LOG.info("Caught exception: " + ExceptionUtils.getFullStackTrace(throwable));
        }
    }

    public void killOrThrow() throws Exception {
        cluster.killOrThrow(name);
    }
}
