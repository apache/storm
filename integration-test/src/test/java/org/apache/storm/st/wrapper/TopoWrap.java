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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.st.utils.AssertUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.StormSubmitter;
import org.apache.thrift.TException;
import org.apache.storm.st.topology.window.data.FromJson;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.st.utils.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;

public class TopoWrap {
    private static Logger log = LoggerFactory.getLogger(TopoWrap.class);
    private final StormCluster cluster;

    private final String name;
    private final StormTopology topology;
    private String id;
    public static Map<String, Object> submitConf = getSubmitConf();
    static {
        String jarFile = getJarPath();
        log.info("setting storm.jar to: " + jarFile);
        System.setProperty("storm.jar", jarFile);
    }

    public TopoWrap(StormCluster cluster, String name, StormTopology topology) {
        this.cluster = cluster;
        this.name = name;
        this.topology = topology;
    }

    public void submit(ImmutableMap<String, Object> of) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        final HashMap<String, Object> newConfig = new HashMap<>(submitConf);
        newConfig.putAll(of);
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
        final Collection<File> jarFiles = Collections2.filter(allJars, new Predicate<File>() {
            @Override
            public boolean apply(@Nullable File input) {
                return input != null && !input.getName().contains("surefirebooter");
            }
        });
        log.info("Found jar files: " + jarFiles);
        AssertUtil.nonEmpty(jarFiles, "The jar file is missing - did you run 'mvn clean package -DskipTests' before running tests ?");
        String jarFile = null;
        for (File jarPath : jarFiles) {
            log.info("jarPath = " + jarPath);
            if (jarPath != null && !jarPath.getPath().contains("original")) {
                AssertUtil.exists(jarPath);
                jarFile = jarPath.getAbsolutePath();
                break;
            }
        }
        Assert.assertNotNull(jarFile, "Couldn't detect a suitable jar file for uploading.");
        log.info("jarFile = " + jarFile);
        return jarFile;
    }

    public void submitSuccessfully(ImmutableMap<String, Object> config) throws TException {
        submit(config);
        TopologySummary topologySummary = getSummary();
        Assert.assertEquals(topologySummary.get_status().toLowerCase(), "active", "Topology must be active.");
        id = topologySummary.get_id();
    }

    public void submitSuccessfully() throws TException {
        submitSuccessfully(ImmutableMap.<String, Object>of());
    }

    private TopologySummary getSummary() throws TException {
        List<TopologySummary> allTopos = cluster.getSummaries();
        Collection<TopologySummary> oneTopo = Collections2.filter(allTopos, new Predicate<TopologySummary>() {
            @Override
            public boolean apply(@Nullable TopologySummary input) {
                return input != null && input.get_name().equals(name);
            }
        });
        AssertUtil.assertOneElement(oneTopo);
        return oneTopo.iterator().next();
    }

    public TopologyInfo getInfo() throws TException {
        return cluster.getNimbusClient().getTopologyInfo(id);
    }

    public long getAllTimeEmittedCount(final String componentId) throws TException {
        TopologyInfo info = getInfo();
        final List<ExecutorSummary> executors = info.get_executors();
        List<Long> ackCounts = Lists.transform(executors, new Function<ExecutorSummary, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable ExecutorSummary input) {
                if (input == null || !input.get_component_id().equals(componentId))
                    return 0L;
                String since = ":all-time";
                return getEmittedCount(input, since);
            }

            //possible values for since are strings :all-time, 600, 10800, 86400
            public Long getEmittedCount(@Nonnull ExecutorSummary input, @Nonnull String since) {
                ExecutorStats executorStats = input.get_stats();
                if (executorStats == null)
                    return 0L;
                Map<String, Map<String, Long>> emitted = executorStats.get_emitted();
                if (emitted == null)
                    return 0L;
                Map<String, Long> allTime = emitted.get(since);
                if (allTime == null)
                    return 0L;
                return allTime.get(Utils.DEFAULT_STREAM_ID);
            }
        });
        return sum(ackCounts).longValue();
    }

    public List<ExecutorURL> getLogUrls(final String componentId) throws TException, MalformedURLException {
        ComponentPageInfo componentPageInfo = cluster.getNimbusClient().getComponentPageInfo(id, componentId, null, false);
        Map<String, ComponentAggregateStats> windowToStats = componentPageInfo.get_window_to_stats();
        ComponentAggregateStats allTimeStats = windowToStats.get(":all-time");
        //Long emitted = (Long) allTimeStats.getFieldValue(ComponentAggregateStats._Fields.findByName("emitted"));


        List<ExecutorAggregateStats> execStats = componentPageInfo.get_exec_stats();
        Set<ExecutorURL> urls = new HashSet<>();
        for (ExecutorAggregateStats execStat : execStats) {
            ExecutorSummary execSummary = execStat.get_exec_summary();
            String host = execSummary.get_host();
            int executorPort = execSummary.get_port();
            //http://supervisor2:8000/download/DemoTest-26-1462229009%2F6703%2Fworker.log
            //http://supervisor2:8000/log?file=SlidingWindowCountTest-9-1462388349%2F6703%2Fworker.log
            int logViewerPort = 8000;
            ExecutorURL executorURL = new ExecutorURL(componentId, host, logViewerPort, executorPort, id);
            urls.add(executorURL);
        }
        return new ArrayList<>(urls);
    }

    public void waitForProgress(int minEmits, String componentName, int maxWaitSec) throws TException {
        for(int i = 0; i < (maxWaitSec+9)/10; ++i) {
            log.info(getInfo().toString());
            long emitCount = getAllTimeEmittedCount(componentName);
            log.info("Count for component " + componentName + " is " + emitCount);
            if (emitCount >= minEmits) {
                break;
            }
            TimeUtil.sleepSec(10);
        }
    }

    public void assertProgress(int minEmits, String componentName, int maxWaitSec) throws TException {
        waitForProgress(minEmits, componentName, maxWaitSec);
        long emitCount = getAllTimeEmittedCount(componentName);
        Assert.assertTrue(emitCount >= minEmits, "Count for component " + componentName + " is " + emitCount + " min is " + minEmits);
    }

    public static class ExecutorURL {
        private String componentId;
        private URL viewUrl;
        private URL downloadUrl;

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
            String sep = "%2F"; //hex of "/"
            String viewUrlStr = String.format("http://%s:%s/api/v1/log?file=", host, logViewerPort);
            String downloadUrlStr = String.format("http://%s:%s/api/v1/download?file=%%2F", host, logViewerPort);
            viewUrl = new URL(String.format("%s/%s%s%d%sworker.log", viewUrlStr, topoId, sep, executorPort, sep));
            downloadUrl = new URL(String.format("%s/%s%s%d%sworker.log", downloadUrlStr, topoId, sep, executorPort, sep));
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

    public <T extends FromJson<T>> List<T> getLogData(final String componentId, final FromJson<T> cls) 
            throws IOException, TException, MalformedURLException {
        final List<LogData> logData = getLogData(componentId);
        return deserializeLogData(logData, cls);
    }
    
    public <T extends FromJson<T>> List<T> deserializeLogData(final List<LogData> logData, final FromJson<T> cls) {
        final List<T> data = new ArrayList<>(
                Collections2.transform(logData, new Function<LogData, T>() {
                    @Nullable
                    @Override
                    public T apply(@Nullable LogData input) {
                        Assert.assertNotNull(input, "Expected LogData to be non-null.");
                        return cls.fromJson(input.getData());
                    }
                }));
        return data;
    }

    public List<LogData> getLogData(final String componentId) throws IOException, TException, MalformedURLException {
        final String logs = getLogs(componentId);
        final String dateRegex = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}";
        Pattern pattern = Pattern.compile("(?=\\n" + dateRegex + ")");
        final String[] strings = pattern.split(logs);
        final Collection<String> interestingLogs = Collections2.filter(Arrays.asList(strings), new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && StringDecorator.isDecorated(input);
            }
        });
        final Collection<LogData> logData = Collections2.transform(interestingLogs, new Function<String, LogData>() {
            @Nullable
            @Override
            public LogData apply(@Nullable String input) {
                return new LogData(input);
            }
        });
        final ArrayList<LogData> sortedLogs = new ArrayList<>(logData);
        Collections.sort(sortedLogs);
        log.info("Found " + sortedLogs.size() + " items for component: " + componentId);
        return sortedLogs;
    }

    public String getLogs(final String componentId) throws IOException, TException, MalformedURLException {
        log.info("Fetching logs for componentId = " + componentId);
        List<ExecutorURL> exclaim2Urls = getLogUrls(componentId);
        log.info("Found " + exclaim2Urls.size() + " urls: " + exclaim2Urls.toString());
        List<String> urlContents = new ArrayList<>();
        for(ExecutorURL executorUrl : exclaim2Urls) {
            if(executorUrl == null || executorUrl.getDownloadUrl() == null) {
                continue;
            }
            log.info("Fetching: " + executorUrl);
            URL downloadUrl = executorUrl.downloadUrl;
            String urlContent = IOUtils.toString(downloadUrl, StandardCharsets.UTF_8);
            urlContents.add(urlContent);
            if (urlContent.length() < 500) {
                log.info("Fetched: " + urlContent);
            } else {
                log.info("Fetched: " + NumberFormat.getNumberInstance(Locale.US).format(urlContent.length()) + " bytes.");
            }
            if (System.getProperty("regression.downloadWorkerLogs").equalsIgnoreCase("true")) {
                final String userDir = System.getProperty("user.dir");
                final File target = new File(userDir, "target");
                final File logDir = new File(target, "logs");
                final File logFile = new File(logDir, downloadUrl.getHost() + "-" + downloadUrl.getFile().split("/")[2]);
                try {
                    FileUtils.forceMkdir(logDir);
                    FileUtils.write(logFile, urlContent, StandardCharsets.UTF_8);
                } catch (Throwable throwable) {
                    log.info("Caught exception: " + ExceptionUtils.getFullStackTrace(throwable));
                }
            }
        }
        return StringUtils.join(urlContents, '\n');
    }

    private Number sum(Collection<? extends Number> nums) {
        Double retVal = 0.0;
        for (Number num : nums) {
            if(num != null) {
                retVal += num.doubleValue();
            }
        }
        return retVal;
    }

    public void killQuietly() {
        cluster.killSilently(name);
    }
}
