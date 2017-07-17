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

package org.apache.storm.daemon.logviewer.handler;

import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.logviewer.LogviewerConstant;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.ui.InvalidRequestException;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple3;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(Enclosed.class)
public class LogviewerLogSearchHandlerTest {

    public static class SearchViaRestApi {
        private String pattern = "needle";
        private String expectedHost = "dev.null.invalid";
        private Integer expectedPort = 8888;
        private String logviewerUrlPrefix = "http://" + expectedHost + ":" + expectedPort;

        /*
         When we click a link to the logviewer, we expect the match line to
         be somewhere near the middle of the page.  So we subtract half of
         the default page length from the offset at which we found the
         match.
         */
        private Function<Integer, Integer> expOffsetFn = arg -> (LogviewerConstant.DEFAULT_BYTES_PER_PAGE / 2 - arg);

        @Test(expected = RuntimeException.class)
        public void testSearchViaRestApiThrowsIfBogusFileIsGiven() throws InvalidRequestException {
            LogviewerLogSearchHandler handler = getSearchHandler();
            handler.substringSearch(null, "a string");
        }

        @Test
        public void testLogviewerLinkCentersTheMatchInThePage() throws UnknownHostException {
            String expectedFname = "foobar.log";

            LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                String actualUrl = handler.urlToMatchCenteredInLogPage(new byte[42], expectedFname, 27526, 8888);

                assertEquals("http://" + expectedHost + ":" + expectedPort + "/api/v1/log?file=" + expectedFname
                        + "&start=1947&length=" + LogviewerConstant.DEFAULT_BYTES_PER_PAGE, actualUrl);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @Test
        public void testLogviewerLinkCentersTheMatchInThePageDaemon() throws UnknownHostException {
            String expectedFname = "foobar.log";

            LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                String actualUrl = handler.urlToMatchCenteredInLogPageDaemonFile(new byte[42], expectedFname, 27526, 8888);

                assertEquals("http://" + expectedHost + ":" + expectedPort + "/api/v1/daemonlog?file=" + expectedFname
                        + "&start=1947&length=" + LogviewerConstant.DEFAULT_BYTES_PER_PAGE, actualUrl);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @SuppressWarnings("checkstyle:LineLength")
        @Test
        public void testReturnsCorrectBeforeAndAfterContext() throws InvalidRequestException, UnknownHostException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                    "logviewer-search-context-tests.log.test");

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(0, "",
                        " needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle ",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                        ));

                matches.add(buildMatchData(7, "needle ",
                        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle needle\n",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                        ));

                matches.add(buildMatchData(127,
                        "needle needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        " needle\n",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                        ));

                matches.add(buildMatchData(134,
                        " needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle ",
                        "\n",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                        ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @Test
        public void testAreallySmallLogFile() throws InvalidRequestException, UnknownHostException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "small-worker.log.test");

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(7, "000000 ",
                        " 000000\n",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @Test
        public void testAreallySmallLogDaemonFile() throws InvalidRequestException, UnknownHostException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "small-worker.log.test");

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "yes");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(7, "000000 ",
                        " 000000\n",
                        pattern,
                        "/api/v1/daemonlog?file=" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearchDaemonLog(file, pattern);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @Test
        public void testNoOffsetReturnedWhenFileEndsOnBufferOffset() throws InvalidRequestException, UnknownHostException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-3072.log.test");

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(3066,
                        Seq.range(0, 128).map(x -> ".").collect(joining()),
                        "",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern);
                Map<String, Object> searchResult2 = handler.substringSearch(file, pattern, 1);

                assertEquals(expected, searchResult);
                assertEquals(expected, searchResult2);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @SuppressWarnings("checkstyle:LineLength")
        @Test
        public void testNextByteOffsetsAreCorrectForEachMatch() throws UnknownHostException, InvalidRequestException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-worker.log.test");

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);

                List<Tuple3<Integer, Integer, Integer>> dataAndExpected = new ArrayList<>();
                // numMatchesSought, numMatchesFound, expectedNextByteOffset
                dataAndExpected.add(new Tuple3<>(1, 1, 11));
                dataAndExpected.add(new Tuple3<>(2, 2, 2042));
                dataAndExpected.add(new Tuple3<>(3, 3, 2052));
                dataAndExpected.add(new Tuple3<>(4, 4, 3078));
                dataAndExpected.add(new Tuple3<>(5, 5, 3196));
                dataAndExpected.add(new Tuple3<>(6, 6, 3202));
                dataAndExpected.add(new Tuple3<>(7, 7, 6252));
                dataAndExpected.add(new Tuple3<>(8, 8, 6321));
                dataAndExpected.add(new Tuple3<>(9, 9, 6397));
                dataAndExpected.add(new Tuple3<>(10, 10, 6476));
                dataAndExpected.add(new Tuple3<>(11, 11, 6554));
                dataAndExpected.add(new Tuple3<>(12, 12, null));
                dataAndExpected.add(new Tuple3<>(13, 12, null));

                dataAndExpected.forEach(Unchecked.consumer(data -> {
                    Map<String, Object> result = handler.substringSearch(file, pattern, data.v1());
                    assertEquals(data.v3(), result.get("nextByteOffset"));
                    assertEquals(data.v2().intValue(), ((List) result.get("matches")).size());
                }));

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);
                expected.put("nextByteOffset", 6252);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(5,
                        "Test ",
                        " is near the beginning of the file.\nThis file assumes a buffer size of 2048 bytes, a max search string size of 1024 bytes, and a",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(2036,
                        "ng 146\npadding 147\npadding 148\npadding 149\npadding 150\npadding 151\npadding 152\npadding 153\nNear the end of a 1024 byte block, a ",
                        ".\nA needle that straddles a 1024 byte boundary should also be detected.\n\npadding 157\npadding 158\npadding 159\npadding 160\npadding",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(2046,
                        "ding 147\npadding 148\npadding 149\npadding 150\npadding 151\npadding 152\npadding 153\nNear the end of a 1024 byte block, a needle.\nA ",
                        " that straddles a 1024 byte boundary should also be detected.\n\npadding 157\npadding 158\npadding 159\npadding 160\npadding 161\npaddi",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(3072,
                        "adding 226\npadding 227\npadding 228\npadding 229\npadding 230\npadding 231\npadding 232\npadding 233\npadding 234\npadding 235\n\n\nHere a ",
                        " occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: needleneedle\n\npa",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(3190,
                        "\n\n\nHere a needle occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: ",
                        "needle\n\npadding 243\npadding 244\npadding 245\npadding 246\npadding 247\npadding 248\npadding 249\npadding 250\npadding 251\npadding 252\n",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(3196,
                        "e a needle occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: needle",
                        "\n\npadding 243\npadding 244\npadding 245\npadding 246\npadding 247\npadding 248\npadding 249\npadding 250\npadding 251\npadding 252\npaddin",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(6246,
                        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n\nHere are four non-ascii 1-byte UTF-8 characters: αβγδε\n\n",
                        "\n\nHere are four printable 2-byte UTF-8 characters: ¡¢£¤¥\n\nneedle\n\n\n\nHere are four printable 3-byte UTF-8 characters: ऄअ",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                Map<String, Object> searchResult = handler.substringSearch(file, pattern, 7);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @SuppressWarnings("checkstyle:LineLength")
        @Test
        public void testCorrectMatchOffsetIsReturnedWhenSkippingBytes() throws InvalidRequestException, UnknownHostException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-worker.log.test");

                int startByteOffset = 3197;

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", startByteOffset);
                expected.put("nextByteOffset", 6252);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(6246,
                        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n\nHere are four non-ascii 1-byte UTF-8 characters: αβγδε\n\n",
                        "\n\nHere are four printable 2-byte UTF-8 characters: ¡¢£¤¥\n\nneedle\n\n\n\nHere are four printable 3-byte UTF-8 characters: ऄअ",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern, 1, startByteOffset);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @SuppressWarnings("checkstyle:LineLength")
        @Test
        public void testAnotherPatterns1() throws UnknownHostException, InvalidRequestException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-worker.log.test");

                String pattern = Seq.range(0, 1024).map(x -> "X").collect(joining());

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);
                expected.put("nextByteOffset", 6183);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(4075,
                        "\n\nThe following match of 1024 bytes completely fills half the byte buffer.  It is a search substring of the maximum size......\n\n",
                        "\nThe following max-size match straddles a 1024 byte buffer.\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                matches.add(buildMatchData(5159,
                        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\nThe following max-size match straddles a 1024 byte buffer.\n",
                        "\n\nHere are four non-ascii 1-byte UTF-8 characters: αβγδε\n\nneedle\n\nHere are four printable 2-byte UTF-8 characters: ¡¢£¤",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern, 2);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @SuppressWarnings("checkstyle:LineLength")
        @Test
        public void testAnotherPatterns2() throws UnknownHostException, InvalidRequestException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-worker.log.test");
                String pattern = "𐄀𐄁𐄂";

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);
                expected.put("nextByteOffset", 7176);

                List<Map<String, Object>> matches = new ArrayList<>();

                matches.add(buildMatchData(7164,
                        "padding 372\npadding 373\npadding 374\npadding 375\n\nThe following tests multibyte UTF-8 Characters straddling the byte boundary:   ",
                        "\n\nneedle",
                        pattern,
                        "/api/v1/log?file=test%2Fresources%2F" + file.getName() + "&start=0&length=51200"
                ));

                expected.put("matches", matches);

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern, 1);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        @Test
        public void testReturnsZeroMatchesForUnseenPattern() throws UnknownHostException, InvalidRequestException {
            Utils prevUtils = null;
            try {
                Utils mockedUtil = mock(Utils.class);
                prevUtils = Utils.setInstance(mockedUtil);

                String pattern = "Not There";

                when(mockedUtil.hostname()).thenReturn(expectedHost);

                final File file = new File(String.join(File.separator, "src", "test", "resources"),
                        "test-worker.log.test");

                Map<String, Object> expected = new HashMap<>();
                expected.put("isDaemon", "no");
                expected.put("searchString", pattern);
                expected.put("startByteOffset", 0);

                expected.put("matches", Collections.emptyList());

                LogviewerLogSearchHandler handler = getSearchHandlerWithPort(expectedPort);
                Map<String, Object> searchResult = handler.substringSearch(file, pattern);

                assertEquals(expected, searchResult);
            } finally {
                Utils.setInstance(prevUtils);
            }
        }

        private Map<String, Object> buildMatchData(int byteOffset, String beforeString, String afterString,
                                                   String matchString, String logviewerUrlPath) {
            Map<String, Object> match = new HashMap<>();
            match.put("byteOffset", byteOffset);
            match.put("beforeString", beforeString);
            match.put("afterString", afterString);
            match.put("matchString", matchString);
            match.put("logviewerURL", logviewerUrlPrefix + logviewerUrlPath);
            return match;
        }
    }

    public static class FindNMatchesTest {
        /**
         * find-n-matches looks through logs properly.
         */
        @Test
        public void testFindNMatches() {
            List<File> files = new ArrayList<>();
            files.add(new File(String.join(File.separator, "src", "test", "resources"),
                    "logviewer-search-context-tests.log.test"));
            files.add(new File(String.join(File.separator, "src", "test", "resources"),
                    "logviewer-search-context-tests.log.gz"));

            final LogviewerLogSearchHandler handler = getSearchHandler();

            final List<Map<String, Object>> matches1 = handler.findNMatches(files, 20, 0, 0, "needle").getMatches();
            final List<Map<String, Object>> matches2 = handler.findNMatches(files, 20, 0, 126, "needle").getMatches();
            final List<Map<String, Object>> matches3 = handler.findNMatches(files, 20, 1, 0, "needle").getMatches();

            assertEquals(2, matches1.size());
            assertEquals(4, ((List) matches1.get(0).get("matches")).size());
            assertEquals(4, ((List) matches1.get(1).get("matches")).size());
            assertEquals("test/resources/logviewer-search-context-tests.log.test", matches1.get(0).get("fileName"));
            assertEquals("test/resources/logviewer-search-context-tests.log.gz", matches1.get(1).get("fileName"));

            assertEquals(2, ((List) matches2.get(0).get("matches")).size());
            assertEquals(4, ((List) matches2.get(1).get("matches")).size());

            assertEquals(1, matches3.size());
            assertEquals(4, ((List) matches3.get(0).get("matches")).size());
        }
    }

    public static class TestDeepSearchLogs {

        private List<File> logFiles;
        private String topoPath;

        /**
         * Setup test environment for each test.
         */
        @Before
        public void setUp() throws IOException {
            logFiles = new ArrayList<>();
            logFiles.add(new File(String.join(File.separator, "src", "test", "resources"),
                    "logviewer-search-context-tests.log.test"));
            logFiles.add(new File(String.join(File.separator, "src", "test", "resources"),
                    "logviewer-search-context-tests.log.gz"));

            FileAttribute[] attrs = new FileAttribute[0];
            topoPath = Files.createTempDirectory("topoA", attrs).toFile().getCanonicalPath();
            new File(topoPath, "6400").createNewFile();
            new File(topoPath, "6500").createNewFile();
            new File(topoPath, "6600").createNewFile();
            new File(topoPath, "6700").createNewFile();
        }

        /**
         * Clean up test environment.
         */
        @After
        public void tearDown() {
            if (StringUtils.isNotEmpty(topoPath)) {
                try {
                    Utils.forceDelete(topoPath);
                } catch (IOException e) {
                    // ignore...
                }
            }
        }

        @Test
        public void testAllPortsAndSearchArchivedIsTrue() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "*", "20", "199", true, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            verify(handler, times(4)).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, times(4)).logsForPort(anyString(), any(File.class));

            // File offset and byte offset should always be zero when searching multiple workers (multiple ports).
            assertEquals(logFiles, files.getAllValues().get(0));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(0));
            assertEquals("search", search.getAllValues().get(0));
            assertEquals(logFiles, files.getAllValues().get(0));

            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(1));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(1));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(1));
            assertEquals("search", search.getAllValues().get(1));
            assertEquals(logFiles, files.getAllValues().get(1));

            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(2));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(2));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(2));
            assertEquals("search", search.getAllValues().get(2));
            assertEquals(logFiles, files.getAllValues().get(2));

            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(3));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(3));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(3));
            assertEquals("search", search.getAllValues().get(3));
        }

        @Test
        public void testAllPortsAndSearchArchivedIsFalse() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", null, "20", "199", false, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            verify(handler, times(4)).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, times(4)).logsForPort(anyString(), any(File.class));

            // File offset and byte offset should always be zero when searching multiple workers (multiple ports).
            assertEquals(Collections.singletonList(logFiles.get(0)), files.getAllValues().get(0));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(0));
            assertEquals("search", search.getAllValues().get(0));

            assertEquals(Collections.singletonList(logFiles.get(0)), files.getAllValues().get(1));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(1));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(1));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(1));
            assertEquals("search", search.getAllValues().get(1));

            assertEquals(Collections.singletonList(logFiles.get(0)), files.getAllValues().get(2));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(2));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(2));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(2));
            assertEquals("search", search.getAllValues().get(2));

            assertEquals(Collections.singletonList(logFiles.get(0)), files.getAllValues().get(3));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(3));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(3));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(3));
            assertEquals("search", search.getAllValues().get(3));
        }

        @Test
        public void testOnePortAndSearchArchivedIsTrueAndNotFileOffset() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "6700", "0", "0", true, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            verify(handler, times(1)).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, times(2)).logsForPort(anyString(), any(File.class));

            assertEquals(logFiles, files.getAllValues().get(0));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(0));
            assertEquals("search", search.getAllValues().get(0));
        }

        @Test
        public void testOnePortAndSearchArchivedIsTrueAndFileOffsetIs1() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "6700", "1", "0", true, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            verify(handler, times(1)).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, times(2)).logsForPort(anyString(), any(File.class));

            assertEquals(logFiles, files.getAllValues().get(0));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(0));
            assertEquals(Integer.valueOf(1), fileOffset.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(0));
            assertEquals("search", search.getAllValues().get(0));
        }

        @Test
        public void testOnePortAndSearchArchivedIsFalseAndFileOffsetIs1() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "6700", "1", "0", false, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            verify(handler, times(1)).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, times(2)).logsForPort(anyString(), any(File.class));

            // File offset should be zero, since search-archived is false.
            assertEquals(Collections.singletonList(logFiles.get(0)), files.getAllValues().get(0));
            assertEquals(Integer.valueOf(20), numMatches.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), fileOffset.getAllValues().get(0));
            assertEquals(Integer.valueOf(0), offset.getAllValues().get(0));
            assertEquals("search", search.getAllValues().get(0));
        }

        @Test
        public void testOnePortAndSearchArchivedIsTrueAndFileOffsetIs1AndByteOffsetIs100() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "6700", "1", "100", true, null, null);

            verify(handler, times(1)).findNMatches(anyListOf(File.class), anyInt(), anyInt(), anyInt(), anyString());
            verify(handler, times(2)).logsForPort(anyString(), any(File.class));
        }

        @Test
        public void testBadPortAndSearchArchivedIsFalseAndFileOffsetIs1() throws IOException {
            LogviewerLogSearchHandler handler = getStubbedSearchHandler();

            handler.deepSearchLogsForTopology("", null, "search", "20", "2700", "1", "0", false, null, null);

            ArgumentCaptor<List> files = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Integer> numMatches = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> fileOffset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
            ArgumentCaptor<String> search = ArgumentCaptor.forClass(String.class);

            // Called with a bad port (not in the config) No searching should be done.
            verify(handler, never()).findNMatches(files.capture(), numMatches.capture(), fileOffset.capture(),
                    offset.capture(), search.capture());
            verify(handler, never()).logsForPort(anyString(), any(File.class));
        }

        private LogviewerLogSearchHandler getStubbedSearchHandler() {
            Map<String, Object> stormConf = Utils.readStormConfig();
            LogviewerLogSearchHandler handler = new LogviewerLogSearchHandler(stormConf, topoPath, null,
                    new ResourceAuthorizer(stormConf));
            handler = spy(handler);

            doReturn(logFiles).when(handler).logsForPort(anyString(), any(File.class));
            doAnswer(invocationOnMock -> {
                Object[] arguments = invocationOnMock.getArguments();
                int fileOffset = (Integer) arguments[2];
                String search = (String) arguments[4];

                return new LogviewerLogSearchHandler.Matched(fileOffset, search, Collections.emptyList());
            }).when(handler).findNMatches(anyListOf(File.class), anyInt(), anyInt(), anyInt(), anyString());

            return handler;
        }
    }

    private static LogviewerLogSearchHandler getSearchHandler() {
        Map<String, Object> stormConf = Utils.readStormConfig();
        return new LogviewerLogSearchHandler(stormConf, null, null,
                new ResourceAuthorizer(stormConf));
    }

    private static LogviewerLogSearchHandler getSearchHandlerWithPort(int port) {
        Map<String, Object> stormConf = Utils.readStormConfig();
        stormConf.put(DaemonConfig.LOGVIEWER_PORT, port);
        return new LogviewerLogSearchHandler(stormConf, null, null,
                new ResourceAuthorizer(stormConf));
    }

}
