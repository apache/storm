/**
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

package org.apache.storm.loadgen;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metrics server that records and reports metrics for a set of running topologies.
 */
public class LoadMetricsServer extends HttpForwardingMetricsServer {
    private static final Logger LOG = LoggerFactory.getLogger(HttpForwardingMetricsServer.class);

    private static class MemMeasure {
        private long mem = 0;
        private long time = 0;

        synchronized void update(long mem) {
            this.mem = mem;
            time = System.currentTimeMillis();
        }

        public synchronized long get() {
            return isExpired() ? 0L : mem;
        }

        synchronized boolean isExpired() {
            return (System.currentTimeMillis() - time) >= 20000;
        }
    }

    @VisibleForTesting
    static double convert(double value, TimeUnit from, TimeUnit target) {
        if (target.compareTo(from) > 0) {
            return value / from.convert(1, target);
        }
        return value * target.convert(1, from);
    }

    public static class Measurements {
        private final Histogram histo;
        private double uiCompleteLatency;
        private long skippedMaxSpoutMs;
        private double userMs;
        private double sysMs;
        private double gcMs;
        private long memBytes;
        private long uptimeSecs;
        private long timeWindow;
        private long acked;
        private long failed;
        private Set<String> topologyIds;
        private long workers;
        private long executors;
        private long hosts;
        private Map<String, String> congested;

        /**
         * Constructor.
         * @param histo latency histogram.
         * @param userMs user CPU in ms.
         * @param sysMs system CPU in ms.
         * @param gcMs GC CPU in ms.
         */
        public Measurements(long uptimeSecs, long acked, long timeWindow, long failed, Histogram histo,
                            double userMs, double sysMs, double gcMs, long memBytes, Set<String> topologyIds,
                            long workers, long executors, long hosts, Map<String, String> congested, long skippedMaxSpoutMs,
                            double uiCompleteLatency) {
            this.uptimeSecs = uptimeSecs;
            this.acked = acked;
            this.timeWindow = timeWindow;
            this.failed = failed;
            this.userMs = userMs;
            this.sysMs = sysMs;
            this.gcMs = gcMs;
            this.histo = histo;
            this.memBytes = memBytes;
            this.topologyIds = topologyIds;
            this.workers = workers;
            this.executors = executors;
            this.hosts = hosts;
            this.congested = congested;
            this.skippedMaxSpoutMs = skippedMaxSpoutMs;
            this.uiCompleteLatency = uiCompleteLatency;
        }

        /**
         * Default Constructor.
         */
        public Measurements() {
            histo = new Histogram(3600000000000L, 3);
            sysMs = 0;
            userMs = 0;
            gcMs = 0;
            memBytes = 0;
            uptimeSecs = 0;
            timeWindow = 0;
            acked = 0;
            failed = 0;
            topologyIds = new HashSet<>();
            workers = 0;
            executors = 0;
            hosts = 0;
            congested = new HashMap<>();
            skippedMaxSpoutMs = 0;
            uiCompleteLatency = 0.0;
        }

        /**
         * Add other to this.
         * @param other meaurements to add in.
         */
        public void add(Measurements other) {
            histo.add(other.histo);
            sysMs += other.sysMs;
            userMs += other.userMs;
            gcMs += other.gcMs;
            memBytes = Math.max(memBytes, other.memBytes);
            acked += other.acked;
            failed += other.failed;
            uptimeSecs = Math.max(uptimeSecs, other.uptimeSecs);
            timeWindow += other.timeWindow;
            topologyIds.addAll(other.topologyIds);
            workers = Math.max(workers, other.workers);
            executors = Math.max(executors, other.executors);
            hosts = Math.max(hosts, other.hosts);
            congested.putAll(other.congested);
            skippedMaxSpoutMs += other.skippedMaxSpoutMs;
            uiCompleteLatency = Math.max(uiCompleteLatency, other.uiCompleteLatency);
        }

        public double getLatencyAtPercentile(double percential, TimeUnit unit) {
            return convert(histo.getValueAtPercentile(percential), TimeUnit.NANOSECONDS, unit);
        }

        public double getMinLatency(TimeUnit unit) {
            return convert(histo.getMinValue(), TimeUnit.NANOSECONDS, unit);
        }

        public double getMaxLatency(TimeUnit unit) {
            return convert(histo.getMaxValue(), TimeUnit.NANOSECONDS, unit);
        }

        public double getMeanLatency(TimeUnit unit) {
            return convert(histo.getMean(), TimeUnit.NANOSECONDS, unit);
        }

        public double getLatencyStdDeviation(TimeUnit unit) {
            return convert(histo.getStdDeviation(), TimeUnit.NANOSECONDS, unit);
        }

        public double getUiCompleteLatency(TimeUnit unit) {
            return convert(uiCompleteLatency, TimeUnit.MILLISECONDS, unit);
        }

        public double getUserTime(TimeUnit unit) {
            return convert(userMs, TimeUnit.MILLISECONDS, unit);
        }

        public double getSysTime(TimeUnit unit) {
            return convert(sysMs, TimeUnit.MILLISECONDS, unit);
        }

        public double getGc(TimeUnit unit) {
            return convert(gcMs, TimeUnit.MILLISECONDS, unit);
        }

        public double getSkippedMaxSpout(TimeUnit unit) {
            return convert(skippedMaxSpoutMs, TimeUnit.MILLISECONDS, unit);
        }

        public double getMemMb() {
            return memBytes / (1024.0 * 1024.0);
        }

        public long getUptimeSecs() {
            return uptimeSecs;
        }

        public long getCompleted() {
            return histo.getTotalCount();
        }

        public double getCompletedPerSec() {
            return getCompleted() / (double)timeWindow;
        }

        public long getAcked() {
            return acked;
        }

        public double getAckedPerSec() {
            return acked / (double)timeWindow;
        }

        public long getFailed() {
            return failed;
        }

        public long startTime() {
            return uptimeSecs - timeWindow;
        }

        public long endTime() {
            return uptimeSecs;
        }

        public double getTimeWindow() {
            return timeWindow;
        }

        public Set<String> getTopologyIds() {
            return topologyIds;
        }

        public long getWorkers() {
            return workers;
        }

        public long getHosts() {
            return hosts;
        }

        public long getExecutors() {
            return executors;
        }

        public Map<String, String> getCongested() {
            return congested;
        }

        static Measurements combine(List<Measurements> measurements, Integer start, Integer count) {
            if (count == null) {
                count = measurements.size();
            }

            if (start == null) {
                start = measurements.size() - count;
            }
            start = Math.max(0, start);
            count = Math.min(count, measurements.size() - start);

            Measurements ret = new Measurements();
            for (int i = start; i < start + count; i ++) {
                ret.add(measurements.get(i));
            }
            return ret;
        }
    }

    interface MetricResultsReporter {
        void start();

        void reportWindow(Measurements inWindow, List<Measurements> allTime);

        void finish(List<Measurements> allTime) throws Exception;
    }

    private static class NoCloseOutputStream extends FilterOutputStream {
        public NoCloseOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close() {
            //NOOP on purpose
        }
    }

    abstract static class FileReporter implements MetricResultsReporter {
        protected final PrintStream out;
        protected final Map<String, MetricExtractor> allExtractors;
        public final boolean includesSysOutOrError;

        public FileReporter(Map<String, MetricExtractor> allExtractors) throws FileNotFoundException {
            this(null, Collections.emptyMap(), allExtractors);
        }

        public FileReporter(String path, Map<String, String> query,  Map<String, MetricExtractor> allExtractors)
            throws FileNotFoundException {
            boolean append = Boolean.parseBoolean(query.getOrDefault("append", "false"));
            boolean tee = Boolean.parseBoolean(query.getOrDefault("tee", "false"));
            boolean includesSysOutOrError = false;

            OutputStream out = null;
            if (path == null || "/dev/stdout".equals(path)) {
                out = new NoCloseOutputStream(System.out);
                includesSysOutOrError = true;
                tee = false;
            } else if ("/dev/stderr".equals(path)) {
                out = new NoCloseOutputStream(System.err);
                includesSysOutOrError = true;
                tee = false;
            } else {
                out = new FileOutputStream(path, append);
            }

            if (tee) {
                out = new TeeOutputStream(new NoCloseOutputStream(System.out), out);
                includesSysOutOrError = true;
            }
            this.out = new PrintStream(out);
            //Copy it in case we want to modify it
            this.allExtractors = new LinkedHashMap<>(allExtractors);
            this.includesSysOutOrError = includesSysOutOrError;
        }

        @Override
        public void start() {
            //NOOP
        }

        @Override
        public void finish(List<Measurements> allTime) throws Exception {
            if (out != null) {
                out.close();
            }
        }
    }

    private static final Map<String, TimeUnit> UNIT_MAP;

    static {
        HashMap<String, TimeUnit> tmp = new HashMap<>();
        tmp.put("NS", TimeUnit.NANOSECONDS);
        tmp.put("NANO", TimeUnit.NANOSECONDS);
        tmp.put("NANOSEC", TimeUnit.NANOSECONDS);
        tmp.put("NANOSECOND", TimeUnit.NANOSECONDS);
        tmp.put("NANOSECONDS", TimeUnit.NANOSECONDS);
        tmp.put("μS", TimeUnit.MICROSECONDS);
        tmp.put("US", TimeUnit.MICROSECONDS);
        tmp.put("MICRO", TimeUnit.MICROSECONDS);
        tmp.put("MICROSEC", TimeUnit.MICROSECONDS);
        tmp.put("MICROSECOND", TimeUnit.MICROSECONDS);
        tmp.put("MICROSECONDS", TimeUnit.MICROSECONDS);
        tmp.put("MS", TimeUnit.MILLISECONDS);
        tmp.put("MILLI", TimeUnit.MILLISECONDS);
        tmp.put("MILLISEC", TimeUnit.MILLISECONDS);
        tmp.put("MILLISECOND", TimeUnit.MILLISECONDS);
        tmp.put("MILLISECONDS", TimeUnit.MILLISECONDS);
        tmp.put("S", TimeUnit.SECONDS);
        tmp.put("SEC", TimeUnit.SECONDS);
        tmp.put("SECOND", TimeUnit.SECONDS);
        tmp.put("SECONDS", TimeUnit.SECONDS);
        tmp.put("M", TimeUnit.MINUTES);
        tmp.put("MIN", TimeUnit.MINUTES);
        tmp.put("MINUTE", TimeUnit.MINUTES);
        tmp.put("MINUTES", TimeUnit.MINUTES);
        UNIT_MAP = Collections.unmodifiableMap(tmp);
    }

    private static final Map<TimeUnit, String> TIME_UNIT_NAME;

    static {
        HashMap<TimeUnit, String> tmp = new HashMap<>();
        tmp.put(TimeUnit.NANOSECONDS, "ns");
        tmp.put(TimeUnit.MICROSECONDS, "μs");
        tmp.put(TimeUnit.MILLISECONDS, "ms");
        tmp.put(TimeUnit.SECONDS, "s");
        tmp.put(TimeUnit.MINUTES, "m");
        TIME_UNIT_NAME = Collections.unmodifiableMap(tmp);
    }

    private static final Map<String, MetricExtractor> NAMED_EXTRACTORS;

    static {
        //Perhaps there is a better way to do this???
        LinkedHashMap<String, MetricExtractor> tmp = new LinkedHashMap<>();
        tmp.put("start_time",  new MetricExtractor((m, unit) -> m.startTime(),"s"));
        tmp.put("end_time",  new MetricExtractor((m, unit) -> m.endTime(), "s"));
        tmp.put("rate",  new MetricExtractor((m, unit) -> m.getCompletedPerSec(), "tuple/s"));
        tmp.put("mean", new MetricExtractor((m, unit) -> m.getMeanLatency(unit)));
        tmp.put("99%ile", new MetricExtractor((m, unit) -> m.getLatencyAtPercentile(99.0, unit)));
        tmp.put("99.9%ile", new MetricExtractor((m, unit) -> m.getLatencyAtPercentile(99.9, unit)));
        tmp.put("cores", new MetricExtractor(
            (m, unit) -> (m.getSysTime(TimeUnit.SECONDS) + m.getUserTime(TimeUnit.SECONDS)) / m.getTimeWindow(),
            ""));
        tmp.put("mem",  new MetricExtractor((m, unit) -> m.getMemMb(), "MB"));
        tmp.put("failed",  new MetricExtractor((m, unit) -> m.getFailed(), ""));
        tmp.put("median", new MetricExtractor((m, unit) -> m.getLatencyAtPercentile(50, unit)));
        tmp.put("min", new MetricExtractor((m, unit) -> m.getMinLatency(unit)));
        tmp.put("max", new MetricExtractor((m, unit) -> m.getMaxLatency(unit)));
        tmp.put("stddev", new MetricExtractor((m, unit) -> m.getLatencyStdDeviation(unit)));
        tmp.put("user_cpu", new MetricExtractor((m, unit) -> m.getUserTime(unit)));
        tmp.put("sys_cpu", new MetricExtractor((m, unit) -> m.getSysTime(unit)));
        tmp.put("gc_cpu", new MetricExtractor((m, unit) -> m.getGc(unit)));
        tmp.put("skipped_max_spout", new MetricExtractor((m, unit) -> m.getSkippedMaxSpout(unit)));
        tmp.put("acked",  new MetricExtractor((m, unit) -> m.getAcked(), ""));
        tmp.put("acked_rate",  new MetricExtractor((m, unit) -> m.getAckedPerSec(), "tuple/s"));
        tmp.put("completed",  new MetricExtractor((m, unit) -> m.getCompleted(), ""));
        tmp.put("uptime",  new MetricExtractor((m, unit) -> m.getUptimeSecs(), "s"));
        tmp.put("time_window",  new MetricExtractor((m, unit) -> m.getTimeWindow(), "s"));
        tmp.put("ids",  new MetricExtractor((m, unit) -> m.getTopologyIds(), ""));
        tmp.put("congested",  new MetricExtractor((m, unit) -> m.getCongested(), ""));
        tmp.put("workers",  new MetricExtractor((m, unit) -> m.getWorkers(), ""));
        tmp.put("hosts",  new MetricExtractor((m, unit) -> m.getHosts(), ""));
        tmp.put("executors",  new MetricExtractor((m, unit) -> m.getExecutors(), ""));
        String buildVersion = VersionInfo.getBuildVersion();
        tmp.put("storm_version", new MetricExtractor((m, unit) -> buildVersion, ""));
        tmp.put("java_version", new MetricExtractor((m, unit) -> System.getProperty("java.vendor")
            + " " + System.getProperty("java.version"),""));
        tmp.put("os_arch", new MetricExtractor((m, unit) -> System.getProperty("os.arch"), ""));
        tmp.put("os_name", new MetricExtractor((m, unit) -> System.getProperty("os.name"), ""));
        tmp.put("os_version", new MetricExtractor((m, unit) -> System.getProperty("os.version"), ""));
        tmp.put("config_override", new MetricExtractor((m, unit) -> Utils.readCommandLineOpts(), ""));
        tmp.put("ui_complete_latency", new MetricExtractor((m, unit) -> m.getUiCompleteLatency(unit)));
        NAMED_EXTRACTORS = Collections.unmodifiableMap(tmp);
    }

    static class MetricExtractor {
        private final String unit;
        private final BiFunction<Measurements, TimeUnit, Object> func;

        public MetricExtractor(BiFunction<Measurements, TimeUnit, Object> func) {
            this.func = func;
            this.unit = null;
        }

        public MetricExtractor(BiFunction<Measurements, TimeUnit, Object> func, String unit) {
            this.func = func;
            this.unit = unit;
        }

        public Object get(Measurements m, TimeUnit unit) {
            return func.apply(m, unit);
        }

        public String formatName(String name, TimeUnit targetUnit) {
            StringBuilder ret = new StringBuilder();
            ret.append(name);
            if (unit == null || !unit.isEmpty()) {
                ret.append("(");
                if (unit == null) {
                    ret.append(TIME_UNIT_NAME.get(targetUnit));
                } else {
                    ret.append(unit);
                }
                ret.append(")");
            }
            return ret.toString();
        }
    }

    abstract static class ColumnsFileReporter extends FileReporter {
        protected final TimeUnit targetUnit;
        protected final List<String> extractors;
        protected final String meta;
        protected final int precision;
        protected String doubleFormat;

        public ColumnsFileReporter(String path, Map<String, String> query, Map<String, MetricExtractor> extractorsMap)
            throws FileNotFoundException {
            this(path, query, extractorsMap, null);
        }

        public ColumnsFileReporter(String path, Map<String, String> query, Map<String, MetricExtractor> extractorsMap,
                                   String defaultPreceision) throws FileNotFoundException {
            super(path, query, extractorsMap);
            targetUnit = UNIT_MAP.get(query.getOrDefault("time", "MILLISECONDS").toUpperCase());
            if (targetUnit == null) {
                throw new IllegalArgumentException(query.get("time") + " is not a supported time unit");
            }
            if (query.containsKey("columns")) {
                List<String> extractors = handleExtractorCleanup(Arrays.asList(query.get("columns").split("\\s*,\\s*")));

                HashSet<String> notFound = new HashSet<>(extractors);
                notFound.removeAll(allExtractors.keySet());
                if (notFound.size() > 0) {
                    throw new IllegalArgumentException(notFound + " columns are not supported");
                }
                this.extractors = extractors;
            } else {
                //Wrapping it makes it mutable
                extractors = new ArrayList<>(Arrays.asList("start_time", "end_time", "rate",
                    "mean", "99%ile", "99.9%ile", "cores", "mem", "failed", "ids", "congested"));
            }

            if (query.containsKey("extraColumns")) {
                List<String> moreExtractors =
                    handleExtractorCleanup(Arrays.asList(query.get("extraColumns").split("\\s*,\\s*")));
                for (String extractor: moreExtractors) {
                    if (!allExtractors.containsKey(extractor)) {
                        throw new IllegalArgumentException(extractor + " is not a supported column");
                    }
                    if (!extractors.contains(extractor)) {
                        extractors.add(extractor);
                    }
                }
            }
            String strPrecision = query.getOrDefault("precision", defaultPreceision);
            if (strPrecision == null) {
                precision = -1;
                doubleFormat = "%f";
            } else {
                precision = Integer.parseInt(strPrecision);
                doubleFormat = "%." + precision + "f";
            }
            meta = query.get("meta");
        }

        protected List<String> handleExtractorCleanup(List<String> orig) {
            Map<String, Object> stormConfig = Utils.readStormConfig();
            List<String> ret = new ArrayList<>(orig.size());
            for (String extractor: orig) {
                if (extractor.startsWith("conf:")) {
                    String confKey = extractor.substring("conf:".length());
                    Object confValue = stormConfig.get(confKey);
                    allExtractors.put(extractor, new MetricExtractor((m, t) -> confValue, ""));
                    ret.add(extractor);
                } else if (extractor.endsWith("%ile")) {
                    double number = Double.valueOf(extractor.substring(0, extractor.length() - "%ile".length()));
                    allExtractors.put(extractor, new MetricExtractor((m, t) -> m.getLatencyAtPercentile(number, t)));
                    ret.add(extractor);
                } else if ("*".equals(extractor)) {
                    ret.addAll(allExtractors.keySet());
                } else {
                    ret.add(extractor);
                }
            }
            return ret;
        }

        protected String format(Object o) {
            if (o instanceof Double || o instanceof Float) {
                return String.format(doubleFormat, o);
            } else {
                return o == null ? "" : o.toString();
            }
        }
    }


    static class FixedWidthReporter extends ColumnsFileReporter {
        public final String longFormat;
        public final String stringFormat;

        public FixedWidthReporter(String path, Map<String, String> query, Map<String, MetricExtractor> extractorsMap)
            throws FileNotFoundException {
            super(path, query, extractorsMap, "3");
            int columnWidth = Integer.parseInt(query.getOrDefault("columnWidth", "15")) - 1;//Always have a space in between
            doubleFormat = "%," + columnWidth + "." + precision + "f";
            longFormat = "%," + columnWidth + "d";
            stringFormat = "%" + columnWidth + "s";
        }

        public FixedWidthReporter(Map<String, MetricExtractor> allExtractors) throws FileNotFoundException {
            this(null, Collections.emptyMap(), allExtractors);
        }

        @Override
        protected String format(Object o) {
            if (o instanceof Double || o instanceof Float) {
                return String.format(doubleFormat, o);
            } else if (o instanceof Integer || o instanceof Long) {
                return String.format(longFormat, o);
            } else {
                return String.format(stringFormat, o);
            }
        }

        @Override
        public void start() {
            boolean first = true;
            for (String name: extractors) {
                if (!first) {
                    out.print(" ");
                }
                first = false;
                out.print(format(allExtractors.get(name).formatName(name, targetUnit)));
            }
            if (meta != null) {
                out.print(" ");
                out.print(format("meta"));
            }
            out.println();
        }

        @Override
        public void reportWindow(Measurements m, List<Measurements> allTime) {
            boolean first = true;
            for (String name: extractors) {
                if (!first) {
                    out.print(" ");
                }
                first = false;
                out.print(format(allExtractors.get(name).get(m, targetUnit)));
            }
            if (meta != null) {
                out.print(" ");
                out.print(format(meta));
            }
            out.println();
        }
    }

    static class SepValReporter extends ColumnsFileReporter {
        private final String separator;

        public SepValReporter(String separator, String path, Map<String, String> query, Map<String, MetricExtractor> extractorsMap)
            throws FileNotFoundException {
            super(path, query, extractorsMap);
            this.separator = separator;
        }

        @Override
        public void start() {
            boolean first = true;
            for (String name: extractors) {
                if (!first) {
                    out.print(separator);
                }
                first = false;
                out.print(allExtractors.get(name).formatName(name, targetUnit));
            }
            if (meta != null) {
                out.print(separator);
                out.print("meta");
            }
            out.println();
        }

        @Override
        public void reportWindow(Measurements m, List<Measurements> allTime) {
            boolean first = true;
            for (String name: extractors) {
                if (!first) {
                    out.print(separator);
                }
                first = false;
                Object value = allExtractors.get(name).get(m, targetUnit);
                String svalue = format(value);
                out.print(escape(svalue));
            }
            if (meta != null) {
                out.print(separator);
                out.print(escape(meta));
            }
            out.println();
        }

        private String escape(String svalue) {
            return svalue.replace("\\", "\\\\").replace(separator, "\\" + separator);
        }
    }

    static class LegacyReporter extends FileReporter {
        private final TimeUnit targetUnitOverride;

        public LegacyReporter(Map<String, MetricExtractor> allExtractors) throws FileNotFoundException {
            super(allExtractors);
            targetUnitOverride = null;
        }

        public LegacyReporter(String path, Map<String, String> query, Map<String, MetricExtractor> allExtractors)
            throws FileNotFoundException {
            super(path, query, allExtractors);
            if (query.containsKey("time")) {
                targetUnitOverride = UNIT_MAP.get(query.get("time").toUpperCase());
                if (targetUnitOverride == null) {
                    throw new IllegalArgumentException(query.get("time") + " is not a supported time unit");
                }
            } else {
                targetUnitOverride = null;
            }
        }

        @Override
        public void reportWindow(Measurements m, List<Measurements> allTime) {
            TimeUnit nsOr = TimeUnit.NANOSECONDS;
            TimeUnit msOr = TimeUnit.MILLISECONDS;
            if (targetUnitOverride != null) {
                nsOr = targetUnitOverride;
                msOr = targetUnitOverride;
            }

            Measurements total = Measurements.combine(allTime, null, null);
            out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d "
                    + "99%%: %,15.0f 99.9%%: %,15.0f min: %,15.0f max: %,15.0f mean: %,15.2f "
                    + "stddev: %,15.2f user: %,10.0f sys: %,10.0f gc: %,10.0f mem: %,10.2f\n",
                m.getUptimeSecs(), m.getAcked(), m.getAckedPerSec(), total.getFailed(),
                m.getLatencyAtPercentile(99.0, nsOr),
                m.getLatencyAtPercentile(99.9, nsOr),
                m.getMinLatency(nsOr),
                m.getMaxLatency(nsOr),
                m.getMeanLatency(nsOr),
                m.getLatencyStdDeviation(nsOr),
                m.getUserTime(msOr),
                m.getSysTime(msOr),
                m.getGc(msOr),
                m.getMemMb());
        }
    }

    /**
     * Add Command line options for configuring the output of this.
     * @param options command line options to update
     */
    public static void addCommandLineOptions(Options options) {
        //We want to be able to select the measurement interval
        // reporting window (We don't need 3 different reports)
        // We want to be able to specify format (and configs specific to the format)
        // With perhaps defaults overall
        options.addOption(Option.builder("r")
            .longOpt("report-interval")
            .hasArg()
            .argName("SECS")
            .desc("How long in between reported metrics.  Will be rounded up to the next 10 sec boundary.\n"
                + "default " + DEFAULT_REPORT_INTERVAL)
            .build());

        options.addOption(Option.builder("w")
            .longOpt("report-window")
            .hasArg()
            .argName("SECS")
            .desc("How long of a rolling window should be in each report.  Will be rounded up to the next report interval boundary.\n"
                + "default " + DEFAULT_WINDOW_INTERVAL)
            .build());

        options.addOption(Option.builder()
            .longOpt("reporter")
            .hasArg()
            .argName("TYPE:PATH?OPTIONS")
            .desc("Provide the config for a reporter to run.  Supported types are:\n"
                + "FIXED - a fixed width format that should be more human readable\n"
                + "LEGACY - (write things out in the legacy format)\n"
                + "TSV - tab separated values\n"
                + "CSV - comma separated values\n"
                + "PATH and OPTIONS are each optional but must be marked with a ':' or '?' separator respectively.")
            .build());

    }

    public static final long DEFAULT_REPORT_INTERVAL = 30;
    public static final long DEFAULT_WINDOW_INTERVAL = DEFAULT_REPORT_INTERVAL;
    private static final Pattern REPORTER_PATTERN = Pattern.compile(
        "(?<type>[^:?]+)(?::(?<path>[^?]+))?(?:\\?(?<query>.*))?");

    private final Histogram histo = new Histogram(3600000000000L, 3);
    private final AtomicLong systemCpu = new AtomicLong(0);
    private final AtomicLong userCpu = new AtomicLong(0);
    private final AtomicLong gcCount = new AtomicLong(0);
    private final AtomicLong gcMs = new AtomicLong(0);
    private final AtomicLong skippedMaxSpoutMs = new AtomicLong(0);
    private final ConcurrentHashMap<String, MemMeasure> memoryBytes = new ConcurrentHashMap<>();
    private final AtomicReference<ConcurrentHashMap<String, String>> congested = new AtomicReference<>(new ConcurrentHashMap<>());
    private final List<MetricResultsReporter> reporters;
    private long prevAcked = 0;
    private long prevFailed = 0;
    private long prevUptime = 0;
    private int windowLength = 1;
    private long reportIntervalSecs = DEFAULT_REPORT_INTERVAL;

    private final LinkedList<Measurements> allCombined = new LinkedList<>();

    LoadMetricsServer(Map<String, Object> conf, CommandLine commandLine, Map<String, Object> parameterMetrics) throws URISyntaxException,
        FileNotFoundException {
        super(conf);
        Map<String, MetricExtractor> allExtractors = new LinkedHashMap<>(NAMED_EXTRACTORS);
        for (Map.Entry<String, Object> entry: parameterMetrics.entrySet()) {
            final Object value = entry.getValue();
            allExtractors.put(entry.getKey(), new MetricExtractor((m, unit) -> value, ""));
        }
        if (commandLine.hasOption("r")) {
            reportIntervalSecs = Long.parseLong(commandLine.getOptionValue("r"));
            reportIntervalSecs = ((reportIntervalSecs + 1) / 10) * 10;
        }
        if (commandLine.hasOption("w")) {
            long window = Long.parseLong(commandLine.getOptionValue("w"));
            windowLength = (int) ((window + 1) / reportIntervalSecs);
        }
        reporters = new ArrayList<>();
        if (commandLine.hasOption("reporter")) {
            for (String reporterString: commandLine.getOptionValues("reporter")) {
                Matcher m = REPORTER_PATTERN.matcher(reporterString);
                if (!m.matches()) {
                    throw new IllegalArgumentException(reporterString + " does not look like it is a reporter");
                }
                String type = m.group("type");
                String path = m.group("path");
                Map<String, String> query = new HashMap<>();
                String queryString = m.group("query");
                if (queryString != null) {
                    for (String param : queryString.split("&")) {
                        String[] pair = param.split("=");
                        String key = pair[0];
                        String value = pair.length > 1 ? pair[1] : "true";
                        query.put(key, value);
                    }
                }
                type = type.toUpperCase();
                switch (type) {
                    case "FIXED":
                        reporters.add(new FixedWidthReporter(path, query, allExtractors));
                        break;
                    case "LEGACY":
                        reporters.add(new LegacyReporter(path, query, allExtractors));
                        break;
                    case "TSV":
                        reporters.add(new SepValReporter("\t", path, query, allExtractors));
                        break;
                    case "CSV":
                        reporters.add(new SepValReporter(",", path, query, allExtractors));
                        break;
                    default:
                        throw new RuntimeException(type + " is not a supported reporter type");
                }
            }
        }
        boolean foundStdOutOrErr = false;
        for (MetricResultsReporter rep : reporters) {
            if (rep instanceof FileReporter) {
                foundStdOutOrErr = ((FileReporter) rep).includesSysOutOrError;
                if (foundStdOutOrErr) {
                    break;
                }
            }
        }
        if (!foundStdOutOrErr) {
            reporters.add(new FixedWidthReporter(allExtractors));
        }
    }

    private long readMemory() {
        long total = 0;
        for (MemMeasure mem: memoryBytes.values()) {
            total += mem.get();
        }
        return total;
    }

    private void startMetricsOutput() {
        for (MetricResultsReporter reporter: reporters) {
            reporter.start();
        }
    }

    private void finishMetricsOutput() throws Exception {
        for (MetricResultsReporter reporter: reporters) {
            reporter.finish(allCombined);
        }
    }

    /**
     * Monitor the list of topologies for the given time frame.
     * @param execTimeMins how long to monitor for
     * @param client the client to use when monitoring
     * @param topoNames the names of the topologies to monitor
     * @throws Exception on any error
     */
    public void monitorFor(double execTimeMins, Nimbus.Iface client, Collection<String> topoNames) throws Exception {
        startMetricsOutput();
        long iterations = (long) ((execTimeMins * 60) / reportIntervalSecs);
        for (int i = 0; i < iterations; i++) {
            Thread.sleep(reportIntervalSecs * 1000);
            outputMetrics(client, topoNames);
        }
        finishMetricsOutput();
    }

    private void outputMetrics(Nimbus.Iface client, Collection<String> names) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        Set<String> ids = new HashSet<>();
        for (TopologySummary ts: summary.get_topologies()) {
            if (names.contains(ts.get_name())) {
                ids.add(ts.get_id());
            }
        }
        if (ids.size() != names.size()) {
            throw new Exception("Could not find all topologies: " + names);
        }
        HashSet<String> workers = new HashSet<>();
        HashSet<String> hosts = new HashSet<>();
        int executors = 0;
        int uptime = 0;
        long acked = 0;
        long failed = 0;
        double totalLatMs = 0;
        long totalLatCount = 0;
        for (String id: ids) {
            TopologyInfo info = client.getTopologyInfo(id);
            @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
            TopologyPageInfo tpi = client.getTopologyPageInfo(id, ":all-time", false);
            uptime = Math.max(uptime, info.get_uptime_secs());
            for (ExecutorSummary exec : info.get_executors()) {
                hosts.add(exec.get_host());
                workers.add(exec.get_host() + exec.get_port());
                executors++;
                if (exec.get_stats() != null && exec.get_stats().get_specific() != null
                    && exec.get_stats().get_specific().is_set_spout()) {
                    SpoutStats stats = exec.get_stats().get_specific().get_spout();
                    Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                    Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                    if (ackedMap != null) {
                        for (String key : ackedMap.keySet()) {
                            if (failedMap != null) {
                                Long tmp = failedMap.get(key);
                                if (tmp != null) {
                                    failed += tmp;
                                }
                            }
                            long ackVal = ackedMap.get(key);
                            acked += ackVal;
                        }
                    }
                }
            }
            Double latency = tpi.get_topology_stats().get_window_to_complete_latencies_ms().get(":all-time");
            Long latAcked = tpi.get_topology_stats().get_window_to_acked().get(":all-time");
            if (latency != null && latAcked != null) {
                totalLatCount += latAcked;
                totalLatMs += (latAcked * latency);
            }
        }
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        long failedThisTime = failed - prevFailed;
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        long ackedThisTime = acked - prevAcked;
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        long thisTime = uptime - prevUptime;
        prevUptime = uptime;
        prevAcked = acked;
        prevFailed = failed;

        Histogram copy = new Histogram(3600000000000L, 3);;
        synchronized (histo) {
            copy.add(histo);
            histo.reset();
        }
        long user = userCpu.getAndSet(0);
        long sys = systemCpu.getAndSet(0);
        long gc = gcMs.getAndSet(0);
        long skippedMaxSpout = skippedMaxSpoutMs.getAndSet(0);
        long memBytes = readMemory();

        allCombined.add(new Measurements(uptime, ackedThisTime, thisTime, failedThisTime, copy, user, sys, gc, memBytes,
            ids, workers.size(), executors, hosts.size(), congested.getAndSet(new ConcurrentHashMap<>()), skippedMaxSpout,
            totalLatMs / totalLatCount));
        Measurements inWindow = Measurements.combine(allCombined, null, windowLength);
        for (MetricResultsReporter reporter: reporters) {
            reporter.reportWindow(inWindow, allCombined);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handle(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints, String topologyId) {
        String worker = taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort;
        for (IMetricsConsumer.DataPoint dp: dataPoints) {
            if (dp.name.startsWith("comp-lat-histo") && dp.value instanceof Histogram) {
                synchronized (histo) {
                    histo.add((Histogram)dp.value);
                }
            } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                Map<Object, Object> m = (Map<Object, Object>)dp.value;
                Object sys = m.get("sys-ms");
                if (sys instanceof Number) {
                    systemCpu.getAndAdd(((Number)sys).longValue());
                }
                Object user = m.get("user-ms");
                if (user instanceof Number) {
                    userCpu.getAndAdd(((Number)user).longValue());
                }
            } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                Map<Object, Object> m = (Map<Object, Object>)dp.value;
                Object count = m.get("count");
                if (count instanceof Number) {
                    gcCount.getAndAdd(((Number)count).longValue());
                }
                Object time = m.get("timeMs");
                if (time instanceof Number) {
                    gcMs.getAndAdd(((Number)time).longValue());
                }
            } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                Map<Object, Object> m = (Map<Object, Object>)dp.value;
                Object val = m.get("usedBytes");
                if (val instanceof Number) {
                    MemMeasure mm = memoryBytes.get(worker);
                    if (mm == null) {
                        mm = new MemMeasure();
                        MemMeasure tmp = memoryBytes.putIfAbsent(worker, mm);
                        mm = tmp == null ? mm : tmp;
                    }
                    mm.update(((Number)val).longValue());
                }
            } else if (dp.name.equals("__receive")) {
                Map<Object, Object> m = (Map<Object, Object>)dp.value;
                Object pop = m.get("population");
                Object cap = m.get("capacity");
                if (pop instanceof Number && cap instanceof Number) {
                    double full = ((Number) pop).doubleValue() / ((Number) cap).doubleValue();
                    if (full >= 0.8) {
                        congested.get().put(
                            topologyId + ":" + taskInfo.srcComponentId + ":" + taskInfo.srcTaskId,
                            "receive " + pop + "/" + cap);
                    }
                }
            } else if (dp.name.equals("__skipped-max-spout-ms")) {
                if (dp.value instanceof Number) {
                    skippedMaxSpoutMs.getAndAdd(((Number) dp.value).longValue());
                    double full = ((Number) dp.value).doubleValue() / 10_000.0; //The frequency of reporting
                    if (full >= 0.8) {
                        congested.get().put(
                            topologyId + ":" + taskInfo.srcComponentId + ":" + taskInfo.srcTaskId,
                            "max.spout.pending " + (int)(full * 100) + "%");
                    }
                }
            }
        }
    }
}
