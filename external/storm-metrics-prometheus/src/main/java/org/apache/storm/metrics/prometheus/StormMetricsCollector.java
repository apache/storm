/*
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

package org.apache.storm.metrics.prometheus;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.PrometheusNaming;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.Quantiles;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.storm.metrics2.MetricRegistryProvider;
import org.apache.storm.metrics2.TaskMetricDimensions;
import org.apache.storm.metrics2.TaskMetricRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns the V2 metrics of one or more workers into Prometheus metric families. The dimensions
 * Storm keeps for each task (topology, host, port, component, task and stream) become labels, so
 * that a metric reported by several tasks ends up in a single family:
 *
 * <ul>
 *   <li>a scoped metric such as {@code fetcher_counter.status_200} becomes {@code
 *       fetcher_counter} with the label {@code scope="status_200"};</li>
 *   <li>a gauge whose value is a map, such as {@code status.count}, becomes {@code status_count}
 *       with a {@code key} label per entry;</li>
 *   <li>a Storm built-in metric such as {@code __ack-count-spout:default} becomes {@code
 *       storm_ack_count} with the labels {@code source_component="spout"} and {@code
 *       stream="default"}.</li>
 * </ul>
 *
 * <p>Counters and meters are exposed as counters, histograms and timers as summaries along with a
 * {@code _max} and a {@code _mean} gauge. Timers are converted to seconds.
 */
class StormMetricsCollector implements MultiCollector {

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsCollector.class);

    private static final double[] QUANTILES = {0.5, 0.75, 0.95, 0.98, 0.99, 0.999};

    private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    /** Prefix of the metrics Storm registers for each task. */
    private static final String STORM_PREFIX = "__";

    /**
     * Storm registers a gauge with this suffix next to each of its counters. Prometheus computes
     * rates from the counter itself.
     */
    private static final String RATE_SUFFIX = ".m1_rate";

    private static final Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

    /** The metrics of a worker. */
    static final class Source {
        private final MetricRegistryProvider provider;
        private final String topology;
        private final MetricFilter filter;

        Source(MetricRegistryProvider provider, String topology, MetricFilter filter) {
            this.provider = provider;
            this.topology = topology;
            this.filter = filter;
        }
    }

    /** Family name and labels derived from the name of a Storm metric. */
    record Name(String family, Labels labels) {
    }

    private enum Type {
        COUNTER,
        GAUGE,
        SUMMARY
    }

    private final List<Source> sources = new CopyOnWriteArrayList<>();

    /** Families reported with conflicting types, so that the conflict is logged only once. */
    private final Set<String> conflicts = ConcurrentHashMap.newKeySet();

    /** {@link TaskMetricRepo} only hands its metrics over to a {@link ScheduledReporter}. */
    private final Capture capture = new Capture();

    void add(Source source) {
        sources.add(source);
    }

    void remove(Source source) {
        sources.remove(source);
    }

    boolean isEmpty() {
        return sources.isEmpty();
    }

    @Override
    public synchronized MetricSnapshots collect() {
        Families families = new Families(conflicts);
        for (Source source : sources) {
            for (Map.Entry<TaskMetricDimensions, TaskMetricRepo> task
                    : source.provider.getTaskMetrics().entrySet()) {
                Map<String, String> dimensions = task.getKey().getDimensions();
                capture.families = families;
                capture.labels = taskLabels(source.topology, dimensions);
                capture.stream = dimensions.get("streamId");
                task.getValue().report(capture, source.filter);
            }
        }
        capture.families = null;
        return families.build();
    }

    static Labels taskLabels(String topology, Map<String, String> dimensions) {
        Labels.Builder builder = Labels.builder();
        addLabel(builder, "topology", topology);
        addLabel(builder, "host", dimensions.get("hostname"));
        addLabel(builder, "port", dimensions.get("port"));
        addLabel(builder, "component", dimensions.get("componentId"));
        addLabel(builder, "task", dimensions.get("taskid"));
        addLabel(builder, "stream", dimensions.get("streamId"));
        return builder.build();
    }

    private static void addLabel(Labels.Builder builder, String name, String value) {
        if (value != null) {
            builder.label(name, value);
        }
    }

    /**
     * Maps the name of a Storm metric to a family name and labels.
     *
     * @param metric the name of the metric as registered with Storm
     * @param stream the stream the metric was registered for, if any
     */
    static Name parse(String metric, String stream) {
        if (metric.startsWith(STORM_PREFIX)) {
            String base = metric.substring(STORM_PREFIX.length());
            Labels labels = Labels.EMPTY;
            if (stream != null && base.endsWith("-" + stream)) {
                // e.g. __emit-count-default
                base = base.substring(0, base.length() - stream.length() - 1);
            } else if (stream != null && base.endsWith(":" + stream)) {
                // e.g. __ack-count-spout:default, the names of Storm's
                // metrics are made of two words
                base = base.substring(0, base.length() - stream.length() - 1);
                int dash = base.indexOf('-', base.indexOf('-') + 1);
                if (dash > 0) {
                    labels = Labels.of("source_component", base.substring(dash + 1));
                    base = base.substring(0, dash);
                }
            }
            return new Name("storm_" + sanitize(base), labels);
        }
        int dot = metric.indexOf('.');
        if (dot > 0 && dot < metric.length() - 1) {
            return new Name(sanitize(metric.substring(0, dot)),
                    Labels.of("scope", metric.substring(dot + 1)));
        }
        return new Name(sanitize(metric), Labels.EMPTY);
    }

    static String sanitize(String name) {
        String sanitized = INVALID_CHARS.matcher(name).replaceAll("_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        // removes suffixes such as _total which are reserved
        return PrometheusNaming.sanitizeMetricName(sanitized);
    }

    private static Double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof Boolean bool) {
            return bool ? 1d : 0d;
        }
        return null;
    }

    private static GaugeDataPointSnapshot gaugePoint(Labels labels, double value) {
        return GaugeDataPointSnapshot.builder().labels(labels).value(value).build();
    }

    /** Converts the metrics of a task. */
    private static final class Capture extends ScheduledReporter {

        private Families families;
        private Labels labels;
        private String stream;

        Capture() {
            super(new MetricRegistry(), "prometheus", MetricFilter.ALL, TimeUnit.SECONDS,
                    TimeUnit.SECONDS);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                           SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                           SortedMap<String, Timer> timers) {
            gauges.forEach((name, gauge) -> {
                // gauges call arbitrary code, one failing must not break the scrape
                try {
                    gauge(name, gauge.getValue());
                } catch (RuntimeException e) {
                    LOG.debug("Could not read gauge {}", name, e);
                }
            });
            counters.forEach((name, counter) -> counter(name, counter.getCount()));
            meters.forEach((name, meter) -> counter(name, meter.getCount()));
            histograms.forEach((name, histogram) ->
                    summary(name, "", histogram.getCount(), histogram.getSnapshot(), 1));
            timers.forEach((name, timer) ->
                    summary(name, "_seconds", timer.getCount(), timer.getSnapshot(),
                            NANOS_PER_SECOND));
        }

        private void gauge(String metric, Object value) {
            if (metric.startsWith(STORM_PREFIX) && metric.endsWith(RATE_SUFFIX)) {
                return;
            }
            if (value instanceof Map<?, ?> map) {
                String family = sanitize(metric);
                map.forEach((key, entry) -> {
                    Double number = toDouble(entry);
                    if (number != null) {
                        Labels keyLabels = labels.add("key", String.valueOf(key));
                        families.add(family, Type.GAUGE, gaugePoint(keyLabels, number));
                    }
                });
                return;
            }
            Double number = toDouble(value);
            if (number == null) {
                return;
            }
            Name name = parse(metric, stream);
            families.add(name.family(), Type.GAUGE,
                    gaugePoint(labels.merge(name.labels()), number));
        }

        private void counter(String metric, long count) {
            Name name = parse(metric, stream);
            families.add(name.family(), Type.COUNTER,
                    CounterDataPointSnapshot.builder()
                            .labels(labels.merge(name.labels()))
                            .value(count)
                            .build());
        }

        private void summary(String metric, String unit, long count, Snapshot snapshot,
                             double scale) {
            Name name = parse(metric, stream);
            String family = name.family() + unit;
            Labels pointLabels = labels.merge(name.labels());
            List<Quantile> quantiles = new ArrayList<>(QUANTILES.length);
            for (double quantile : QUANTILES) {
                quantiles.add(new Quantile(quantile, snapshot.getValue(quantile) / scale));
            }
            families.add(family, Type.SUMMARY,
                    SummaryDataPointSnapshot.builder()
                            .labels(pointLabels)
                            .count(count)
                            .quantiles(Quantiles.of(quantiles))
                            .build());
            families.add(family + "_max", Type.GAUGE,
                    gaugePoint(pointLabels, snapshot.getMax() / scale));
            families.add(family + "_mean", Type.GAUGE,
                    gaugePoint(pointLabels, snapshot.getMean() / scale));
        }
    }

    /** Data points grouped by family. */
    private static final class Families {

        private final Map<String, Family> byName = new TreeMap<>();
        private final Set<String> conflicts;

        Families(Set<String> conflicts) {
            this.conflicts = conflicts;
        }

        void add(String name, Type type, DataPointSnapshot point) {
            Family family = byName.computeIfAbsent(name, n -> new Family(type));
            if (family.type != type) {
                if (conflicts.add(name)) {
                    LOG.warn("Metric family {} is reported both as a {} and a {}, "
                            + "ignoring the latter", name, family.type, type);
                }
                return;
            }
            family.points.putIfAbsent(point.getLabels(), point);
        }

        MetricSnapshots build() {
            MetricSnapshots.Builder builder = MetricSnapshots.builder();
            byName.forEach((name, family) -> builder.metricSnapshot(family.snapshot(name)));
            return builder.build();
        }
    }

    private static final class Family {

        private final Type type;
        private final Map<Labels, DataPointSnapshot> points = new LinkedHashMap<>();

        Family(Type type) {
            this.type = type;
        }

        @SuppressWarnings("unchecked")
        MetricSnapshot snapshot(String name) {
            MetricMetadata metadata = new MetricMetadata(name);
            Collection<? extends DataPointSnapshot> values = points.values();
            if (type == Type.COUNTER) {
                return new CounterSnapshot(metadata, (Collection<CounterDataPointSnapshot>) values);
            }
            if (type == Type.GAUGE) {
                return new GaugeSnapshot(metadata, (Collection<GaugeDataPointSnapshot>) values);
            }
            return new SummarySnapshot(metadata, (Collection<SummaryDataPointSnapshot>) values);
        }
    }
}
