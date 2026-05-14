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

package org.apache.storm.metrics2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EwmaGaugeTest {

    private static final double DELTA = 1e-9;

    @Nested
    @DisplayName("Construction")
    class ConstructionTest {

        @Test
        @DisplayName("Default constructor uses RFC 1889 alpha (1/16)")
        void defaultAlpha() {
            EwmaGauge gauge = new EwmaGauge();
            gauge.addValue(0L);
            gauge.addValue(16L); // D = 16 ; J = 0 + (16 - 0) * (1/16) = 1.0
            assertEquals(1.0, gauge.getValue(), DELTA);
        }

        @Test
        @DisplayName("Invalid alpha values throw IllegalArgumentException")
        void invalidAlphaThrows() {
            double[] invalidAlphas = {
                    0.0, 1.0, -0.1, 1.1,
                    Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY
            };
            for (double alpha : invalidAlphas) {
                assertThrows(IllegalArgumentException.class,
                        () -> new EwmaGauge(alpha),
                        "Expected IllegalArgumentException for alpha=" + alpha);
            }
        }

        @Test
        @DisplayName("Valid alpha boundary values are accepted")
        void validAlphaAccepted() {
            double[] validAlphas = {0.001, 0.0625, 0.5, 0.999};
            for (double alpha : validAlphas) {
                assertNotNull(new EwmaGauge(alpha),
                        "Expected no exception for alpha=" + alpha);
            }
        }
    }


    @Nested
    @DisplayName("Cold-start semantics")
    class ColdStartTest {

        private EwmaGauge gauge;

        @BeforeEach
        void setUp() {
            gauge = new EwmaGauge();
        }

        @Test
        @DisplayName("getValue() returns 0.0 before any sample")
        void noSamples() {
            assertEquals(0.0, gauge.getValue(), DELTA);
        }

        @Test
        @DisplayName("getValue() returns 0.0 after exactly one sample (seed only)")
        void oneSample() {
            gauge.addValue(100L);
            assertEquals(0.0, gauge.getValue(), DELTA);
        }

    }

    @Nested
    @DisplayName("EWMA formula RFC 1889 §A.8")
    class FormulaTest {

        @Test
        @DisplayName("Single update: J = 0 + (D - 0) * alpha")
        void singleDeviation() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L);
            gauge.addValue(10L);
            assertEquals(5.0, gauge.getValue(), DELTA);
        }

        @Test
        @DisplayName("Manual step-by-step verification against reference values")
        void manualSteps() {
            EwmaGauge gauge = new EwmaGauge(0.5);

            gauge.addValue(0L); // seed

            // Step 1: transit=10, prev=0,  D=10, J = 0    + (10-0)    * 0.5 = 5.0
            gauge.addValue(10L);
            assertEquals(5.0, gauge.getValue(), DELTA, "Step 1");

            // Step 2: transit=0,  prev=10, D=10, J = 5.0  + (10-5.0)  * 0.5 = 7.5
            gauge.addValue(0L);
            assertEquals(7.5, gauge.getValue(), DELTA, "Step 2");

            // Step 3: transit=10, prev=0,  D=10, J = 7.5  + (10-7.5)  * 0.5 = 8.75
            gauge.addValue(10L);
            assertEquals(8.75, gauge.getValue(), DELTA, "Step 3");
        }

        @Test
        @DisplayName("Zero deviation decays jitter toward zero")
        void zeroDeviationDecays() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L); // 0
            gauge.addValue(10L); // 0 + 5*alpha = 2.5
            double afterFirst = gauge.getValue();
            assertEquals(afterFirst, gauge.getValue(), DELTA);

            gauge.addValue(10L); // 2.5 - 2.5*alpha = 2.5 - 1.25 = 1.25
            assertEquals(afterFirst * 0.5, gauge.getValue(), DELTA);
        }

    }


    @Nested
    @DisplayName("Negative value guard")
    class NegativeValueTest {

        @Test
        @DisplayName("Negative transit values are silently ignored before seed")
        void negativeIgnoredBeforeSeed() {
            EwmaGauge gauge = new EwmaGauge();
            gauge.addValue(-1L);
            gauge.addValue(-100L);
            assertEquals(0.0, gauge.getValue(), DELTA);
        }

        @Test
        @DisplayName("Negative value after seed does not corrupt lastTransit")
        void negativeAfterSeedIgnored() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(10L);
            gauge.addValue(-5L);
            gauge.addValue(20L);
            assertEquals(5.0, gauge.getValue(), DELTA);
        }
    }


    @Nested
    @DisplayName("getValue() preserves EWMA across calls")
    class GetValueIdempotentTest {

        @Test
        @DisplayName("Repeated getValue() without new samples returns same estimate")
        void repeatedGetValueStable() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L);
            gauge.addValue(10L);
            double first = gauge.getValue();

            assertEquals(first, gauge.getValue(), DELTA, "Second call");
            assertEquals(first, gauge.getValue(), DELTA, "Third call");
        }

        @Test
        @DisplayName("EWMA accumulates correctly across multiple reporting windows")
        void acrossReportingWindows() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L);

            gauge.addValue(10L);
            assertEquals(5.0, gauge.getValue(), DELTA, "Window 1");

            gauge.addValue(0L);
            assertEquals(7.5, gauge.getValue(), DELTA, "Window 2");

            gauge.addValue(10L);
            assertEquals(8.75, gauge.getValue(), DELTA, "Window 3");
        }
    }

    @Nested
    @DisplayName("thread safe")
    class ConcurrencyTest {

        @Test
        @DisplayName("Concurrent addValue() calls do not corrupt state")
        void concurrentAddValue() throws InterruptedException {
            EwmaGauge gauge = new EwmaGauge();
            int threads = 8;
            int samplesPerThread = 10_000;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);

            for (int t = 0; t < threads; t++) {
                final long base = t * 10L;
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int i = 0; i < samplesPerThread; i++) {
                        gauge.addValue(base + (i % 10));
                    }
                });
            }

            ready.await();
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS),
                    "Executor did not terminate — possible deadlock");

            double value = gauge.getValue();
            assertTrue(value >= 0.0, "Jitter must be non-negative, got: " + value);
            assertTrue(Double.isFinite(value), "Jitter must be finite, got: " + value);
        }

        @Test
        @DisplayName("Concurrent getValue() and addValue() do not deadlock")
        void concurrentGetAndAdd() throws Exception {
            EwmaGauge gauge = new EwmaGauge();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch done = new CountDownLatch(2);

            Future<?> writer = pool.submit(() -> {
                for (int i = 0; i < 50_000; i++) {
                    gauge.addValue(i % 100);
                }
                done.countDown();
            });

            Future<?> reader = pool.submit(() -> {
                for (int i = 0; i < 1_000; i++) {
                    double v = gauge.getValue();
                    assertTrue(v >= 0.0 && Double.isFinite(v),
                            "getValue() returned invalid result: " + v);
                }
                done.countDown();
            });

            assertTrue(done.await(10, TimeUnit.SECONDS),
                    "Test did not complete within timeout possible deadlock");

            writer.get();
            reader.get();
            pool.shutdown();
        }

        @Test
        @DisplayName("Only one thread seeds lastTransit all same value gives zero jitter")
        void seedRace() throws InterruptedException {
            EwmaGauge gauge = new EwmaGauge();
            int threads = 16;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);

            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    gauge.addValue(42L);
                });
            }

            ready.await();
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS),
                    "Executor did not terminate possible deadlock");

            assertEquals(0.0, gauge.getValue(), DELTA);
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCaseTest {

        @Test
        @DisplayName("Long.MAX_VALUE transit does not overflow deviation")
        void maxLongTransit() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L);
            gauge.addValue(Long.MAX_VALUE);
            double value = gauge.getValue();
            assertTrue(value > 0.0, "Jitter should be positive");
            assertTrue(Double.isFinite(value), "Jitter should be finite");
        }

        @Test
        @DisplayName("Zero transit time is valid and produces zero deviation")
        void zeroTransit() {
            EwmaGauge gauge = new EwmaGauge(0.5);
            gauge.addValue(0L);
            gauge.addValue(0L);
            assertEquals(0.0, gauge.getValue(), DELTA);
        }

        @Test
        @DisplayName("Large number of samples does not overflow LongAdder")
        void manySamples() {
            EwmaGauge gauge = new EwmaGauge();
            gauge.addValue(0L);
            for (int i = 1; i <= 100_000; i++) {
                gauge.addValue(i % 2 == 0 ? 0L : 10L);
            }
            double value = gauge.getValue();
            assertTrue(value > 0.0, "Jitter should be positive after many samples");
            assertTrue(value <= 10.0, "Jitter cannot exceed max deviation of 10");
            assertTrue(Double.isFinite(value), "Jitter must be finite");
        }
    }
}
