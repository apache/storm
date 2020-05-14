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

import static java.util.stream.Collectors.toList;
import static org.apache.storm.daemon.utils.ListFunctionalSupport.drop;
import static org.apache.storm.daemon.utils.ListFunctionalSupport.first;
import static org.apache.storm.daemon.utils.ListFunctionalSupport.last;
import static org.apache.storm.daemon.utils.ListFunctionalSupport.rest;
import static org.apache.storm.daemon.utils.PathUtil.truncatePathToLastElements;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.common.JsonResponseBuilder;
import org.apache.storm.daemon.logviewer.LogviewerConstant;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.logviewer.utils.LogviewerResponseBuilder;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.ui.InvalidRequestException;
import org.apache.storm.daemon.utils.StreamUtil;
import org.apache.storm.daemon.utils.UrlBuilder;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;
import org.json.simple.JSONAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogviewerLogSearchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogviewerLogSearchHandler.class);
    public static final int GREP_MAX_SEARCH_SIZE = 1024;
    public static final int GREP_BUF_SIZE = 2048;
    public static final int GREP_CONTEXT_SIZE = 128;
    public static final Pattern WORKER_LOG_FILENAME_PATTERN = Pattern.compile("^worker.log(.*)");

    private final Meter numDeepSearchNoResult;
    private final Histogram numFileScanned;
    private final Meter numSearchRequestNoResult;
    private final Meter numFileOpenExceptions;
    private final Meter numFileReadExceptions;

    private final Map<String, Object> stormConf;
    private final Path logRoot;
    private final Path daemonLogRoot;
    private final ResourceAuthorizer resourceAuthorizer;
    private final Integer logviewerPort;
    private final String scheme;
    private final DirectoryCleaner directoryCleaner;

    /**
     * Constructor.
     *
     * @param stormConf storm configuration
     * @param logRoot log root directory
     * @param daemonLogRoot daemon log root directory
     * @param resourceAuthorizer {@link ResourceAuthorizer}
     * @param metricsRegistry The logviewer metrics registry
     */
    public LogviewerLogSearchHandler(Map<String, Object> stormConf, Path logRoot, Path daemonLogRoot,
        ResourceAuthorizer resourceAuthorizer, StormMetricsRegistry metricsRegistry) {
        this.stormConf = stormConf;
        this.logRoot = logRoot.toAbsolutePath().normalize();
        this.daemonLogRoot = daemonLogRoot.toAbsolutePath().normalize();
        this.resourceAuthorizer = resourceAuthorizer;
        Object httpsPort = stormConf.get(DaemonConfig.LOGVIEWER_HTTPS_PORT);
        if (httpsPort == null) {
            this.logviewerPort = ObjectReader.getInt(stormConf.get(DaemonConfig.LOGVIEWER_PORT));
            this.scheme = "http";
        } else {
            this.logviewerPort = ObjectReader.getInt(httpsPort);
            this.scheme = "https";
        }
        this.numDeepSearchNoResult = metricsRegistry.registerMeter("logviewer:num-deep-search-no-result");
        this.numFileScanned = metricsRegistry.registerHistogram("logviewer:num-files-scanned-per-deep-search");
        this.numSearchRequestNoResult = metricsRegistry.registerMeter("logviewer:num-search-request-no-result");
        this.numFileOpenExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_OPEN_EXCEPTIONS);
        this.numFileReadExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_READ_EXCEPTIONS);
        this.directoryCleaner = new DirectoryCleaner(metricsRegistry);
    }

    /**
     * Search from a worker log file.
     *
     * @param fileName log file
     * @param user username
     * @param isDaemon whether the log file is regarding worker or daemon
     * @param search search string
     * @param numMatchesStr the count of maximum matches
     * @param offsetStr start offset for log file
     * @param callback callbackParameterName for JSONP
     * @param origin origin
     * @return Response containing JSON content representing search result
     */
    public Response searchLogFile(String fileName, String user, boolean isDaemon, String search,
        String numMatchesStr, String offsetStr, String callback, String origin)
        throws IOException, InvalidRequestException {
        boolean noResult = true;

        Path rootDir = isDaemon ? daemonLogRoot : logRoot;
        Path rawFile = rootDir.resolve(fileName);
        Path absFile = rawFile.toAbsolutePath().normalize();
        if (!absFile.startsWith(rootDir) || !rawFile.normalize().toString().equals(rawFile.toString())) {
            //Ensure filename doesn't contain ../ parts 
            return searchLogFileNotFound(callback);
        }
        if (isDaemon && Paths.get(fileName).getNameCount() != 1) {
            //Don't permit path traversal for calls intended to read from the daemon logs
            return searchLogFileNotFound(callback);
        }
        Response response;
        if (absFile.toFile().exists()) {
            if (isDaemon || resourceAuthorizer.isUserAllowedToAccessFile(user, fileName)) {
                Integer numMatchesInt = numMatchesStr != null ? tryParseIntParam("num-matches", numMatchesStr) : null;
                Integer offsetInt = offsetStr != null ? tryParseIntParam("start-byte-offset", offsetStr) : null;

                try {
                    if (StringUtils.isNotEmpty(search) && search.getBytes("UTF-8").length <= GREP_MAX_SEARCH_SIZE) {
                        Map<String, Object> entity = new HashMap<>();
                        entity.put("isDaemon", isDaemon ? "yes" : "no");
                        Map<String, Object> res = substringSearch(absFile, search, isDaemon, numMatchesInt, offsetInt);
                        entity.putAll(res);
                        noResult = ((List) res.get("matches")).isEmpty();

                        response = LogviewerResponseBuilder.buildSuccessJsonResponse(entity, callback, origin);
                    } else {
                        throw new InvalidRequestException("Search substring must be between 1 and 1024 "
                            + "UTF-8 bytes in size (inclusive)");
                    }
                } catch (Exception ex) {
                    response = LogviewerResponseBuilder.buildExceptionJsonResponse(ex, callback);
                }
            } else {
                // unauthorized
                response = LogviewerResponseBuilder.buildUnauthorizedUserJsonResponse(user, callback);
            }
        } else {
            response = searchLogFileNotFound(callback);
        }

        if (noResult) {
            numSearchRequestNoResult.mark();
        }
        return response;
    }

    private Response searchLogFileNotFound(String callback) {
        Map<String, String> entity = new HashMap<>();
        entity.put("error", "Not Found");
        entity.put("errorMessage", "The file was not found on this node.");

        return new JsonResponseBuilder().setData(entity).setCallback(callback).setStatus(404).build();
    }

    /**
     * Advanced search across worker log files in a topology.
     *
     * @param topologyId topology ID
     * @param user username
     * @param search search string
     * @param numMatchesStr the count of maximum matches. Note that this number is with respect to each port, not to each log or each search
     *     request
     * @param portStr worker port, null or '*' if the request wants to search from all worker logs
     * @param fileOffsetStr index (offset) of the log files
     * @param offsetStr start offset for log file
     * @param searchArchived true if the request wants to search also archived files, false if not
     * @param callback callbackParameterName for JSONP
     * @param origin origin
     * @return Response containing JSON content representing search result
     */
    public Response deepSearchLogsForTopology(String topologyId, String user, String search,
        String numMatchesStr, String portStr, String fileOffsetStr, String offsetStr,
        Boolean searchArchived, String callback, String origin) throws IOException {
        int numMatchedFiles = 0;
        int numScannedFiles = 0;

        Path rootDir = logRoot;
        Path absTopoDir = rootDir.resolve(topologyId).toAbsolutePath().normalize();
        Object returnValue;
        if (StringUtils.isEmpty(search) || !absTopoDir.toFile().exists() || !absTopoDir.startsWith(rootDir)) {
            returnValue = new ArrayList<>();
        } else {
            int fileOffset = ObjectReader.getInt(fileOffsetStr, 0);
            int offset = ObjectReader.getInt(offsetStr, 0);
            int numMatches = ObjectReader.getInt(numMatchesStr, 1);

            if (StringUtils.isEmpty(portStr) || portStr.equals("*")) {
                try (Stream<Path> topoDir = Files.list(absTopoDir)) {
                    // check for all ports
                    Stream<List<Path>> portsOfLogs = topoDir
                        .map(portDir -> logsForPort(user, portDir))
                        .filter(logs -> logs != null && !logs.isEmpty());

                    if (BooleanUtils.isNotTrue(searchArchived)) {
                        portsOfLogs = portsOfLogs.map(fl -> Collections.singletonList(first(fl)));
                    }

                    final List<Matched> matchedList = portsOfLogs
                        .map(logs -> findNMatches(logs, numMatches, 0, 0, search))
                        .collect(toList());
                    numMatchedFiles = matchedList.stream().mapToInt(match -> match.getMatches().size()).sum();
                    numScannedFiles = matchedList.stream().mapToInt(match -> match.openedFiles).sum();
                    returnValue = matchedList;
                }
            } else {
                int port = Integer.parseInt(portStr);
                // check just the one port
                @SuppressWarnings("unchecked")
                List<Integer> slotsPorts = SupervisorUtils.getSlotsPorts(stormConf);
                boolean containsPort = slotsPorts.stream()
                    .anyMatch(slotPort -> slotPort != null && (slotPort == port));
                if (!containsPort) {
                    returnValue = new ArrayList<>();
                } else {
                    Path absPortDir = absTopoDir.resolve(Integer.toString(port)).toAbsolutePath().normalize();

                    if (!absPortDir.toFile().exists()
                        || !absPortDir.startsWith(absTopoDir)) {
                        returnValue = new ArrayList<>();
                    } else {
                        List<Path> filteredLogs = logsForPort(user, absPortDir);
                        if (BooleanUtils.isNotTrue(searchArchived)) {
                            filteredLogs = Collections.singletonList(first(filteredLogs));
                            fileOffset = 0;
                        }
                        returnValue = findNMatches(filteredLogs, numMatches, fileOffset, offset, search);
                        numMatchedFiles = ((Matched) returnValue).getMatches().size();
                        numScannedFiles = ((Matched) returnValue).openedFiles;
                    }
                }
            }
        }

        if (numMatchedFiles == 0) {
            numDeepSearchNoResult.mark();
        }
        numFileScanned.update(numScannedFiles);
        return LogviewerResponseBuilder.buildSuccessJsonResponse(returnValue, callback, origin);
    }

    private Integer tryParseIntParam(String paramName, String value) throws InvalidRequestException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("Could not parse " + paramName + " to an integer");
        }
    }

    @VisibleForTesting
    Map<String, Object> substringSearch(Path file, String searchString) throws InvalidRequestException {
        return substringSearch(file, searchString, false, 10, 0);
    }

    @VisibleForTesting
    Map<String, Object> substringSearch(Path file, String searchString, int numMatches) throws InvalidRequestException {
        return substringSearch(file, searchString, false, numMatches, 0);
    }

    @VisibleForTesting
    Map<String, Object> substringSearch(Path file,
            String searchString,
            int numMatches,
            int startByteOffset) throws InvalidRequestException {
        return substringSearch(file, searchString, false, numMatches, startByteOffset);
    }

    private Map<String, Object> substringSearch(Path file, String searchString, boolean isDaemon, Integer numMatches,
        Integer startByteOffset) throws InvalidRequestException {
        if (StringUtils.isEmpty(searchString)) {
            throw new IllegalArgumentException("Precondition fails: search string should not be empty.");
        }
        if (searchString.getBytes(StandardCharsets.UTF_8).length > GREP_MAX_SEARCH_SIZE) {
            throw new IllegalArgumentException("Precondition fails: the length of search string should be less than "
                + GREP_MAX_SEARCH_SIZE);
        }

        boolean isZipFile = file.toString().endsWith(".gz");
        try (InputStream fis = Files.newInputStream(file)) {
            try (InputStream gzippedInputStream = isZipFile ? new GZIPInputStream(fis) : fis;
                BufferedInputStream stream = new BufferedInputStream(gzippedInputStream)) {

                //It's more likely to be a file read exception here, so we don't differentiate
                int fileLength = isZipFile ? (int) ServerUtils.zipFileSize(file.toFile()) : (int) Files.size(file);

                ByteBuffer buf = ByteBuffer.allocate(GREP_BUF_SIZE);
                final byte[] bufArray = buf.array();
                final byte[] searchBytes = searchString.getBytes(StandardCharsets.UTF_8);
                numMatches = numMatches != null ? numMatches : 10;
                startByteOffset = startByteOffset != null ? startByteOffset : 0;

                // Start at the part of the log file we are interested in.
                // Allow searching when start-byte-offset == file-len so it doesn't blow up on 0-length files
                if (startByteOffset > fileLength) {
                    throw new InvalidRequestException("Cannot search past the end of the file");
                }

                if (startByteOffset > 0) {
                    StreamUtil.skipBytes(stream, startByteOffset);
                }

                Arrays.fill(bufArray, (byte) 0);

                int totalBytesRead = 0;
                int bytesRead = stream.read(bufArray, 0, Math.min(fileLength, GREP_BUF_SIZE));
                buf.limit(bytesRead);
                totalBytesRead += bytesRead;

                List<Map<String, Object>> initialMatches = new ArrayList<>();
                int initBufOffset = 0;
                int byteOffset = startByteOffset;
                byte[] beforeBytes = null;

                Map<String, Object> ret = new HashMap<>();
                while (true) {
                    SubstringSearchResult searchRet = bufferSubstringSearch(isDaemon, file, fileLength, byteOffset, initBufOffset,
                        stream, startByteOffset, totalBytesRead, buf, searchBytes, initialMatches, numMatches, beforeBytes);

                    List<Map<String, Object>> matches = searchRet.getMatches();
                    Integer newByteOffset = searchRet.getNewByteOffset();
                    byte[] newBeforeBytes = searchRet.getNewBeforeBytes();

                    if (matches.size() < numMatches && totalBytesRead + startByteOffset < fileLength) {
                        // The start index is positioned to find any possible
                        // occurrence search string that did not quite fit in the
                        // buffer on the previous read.
                        final int newBufOffset = Math.min(buf.limit(), GREP_MAX_SEARCH_SIZE) - searchBytes.length;

                        totalBytesRead = rotateGrepBuffer(buf, stream, totalBytesRead, fileLength);
                        if (totalBytesRead < 0) {
                            throw new InvalidRequestException("Cannot search past the end of the file");
                        }

                        initialMatches = matches;
                        initBufOffset = newBufOffset;
                        byteOffset = newByteOffset;
                        beforeBytes = newBeforeBytes;
                    } else {
                        ret.put("isDaemon", isDaemon ? "yes" : "no");
                        Integer nextByteOffset = null;
                        if (matches.size() >= numMatches || totalBytesRead < fileLength) {
                            nextByteOffset = (Integer) last(matches).get("byteOffset") + searchBytes.length;
                            if (fileLength <= nextByteOffset) {
                                nextByteOffset = null;
                            }
                        }
                        ret.putAll(mkGrepResponse(searchBytes, startByteOffset, matches, nextByteOffset));
                        break;
                    }
                }
                return ret;
            } catch (UnknownHostException | UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                numFileReadExceptions.mark();
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            numFileOpenExceptions.mark();
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Map<String, Object> substringSearchDaemonLog(Path file, String searchString) throws InvalidRequestException {
        return substringSearch(file, searchString, true, 10, 0);
    }

    /**
     * Get the filtered, authorized, sorted log files for a port.
     */
    @VisibleForTesting
    List<Path> logsForPort(String user, Path portDir) {
        try {
            List<Path> workerLogs = directoryCleaner.getFilesForDir(portDir).stream()
                .filter(file -> WORKER_LOG_FILENAME_PATTERN.asPredicate().test(file.getFileName().toString()))
                .collect(toList());

            return workerLogs.stream()
                .filter(log -> resourceAuthorizer.isUserAllowedToAccessFile(user, WorkerLogs.getTopologyPortWorkerLog(log)))
                .map(Unchecked.function(p -> Pair.of(p, Files.getLastModifiedTime(p))))
                .sorted(Comparator.comparing((Pair<Path, FileTime> p) -> p.getRight()).reversed())
                .map(p -> p.getLeft())
                .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find the first N matches of target string in files.
     *
     * @param logs all candidate log files to search
     * @param numMatches number of matches expected
     * @param fileOffset number of log files to skip initially
     * @param startByteOffset number of byte to be ignored in each log file
     * @param targetStr searched string
     * @return all matched results
     */
    @VisibleForTesting
    Matched findNMatches(List<Path> logs, int numMatches, int fileOffset, int startByteOffset, String targetStr) {
        logs = drop(logs, fileOffset);
        LOG.debug("{} files to scan", logs.size());

        List<Map<String, Object>> matches = new ArrayList<>();
        int matchCount = 0;
        int scannedFiles = 0;

        while (true) {
            if (logs.isEmpty()) {
                //fileOffset = one past last scanned file
                break;
            }

            Path firstLog = logs.get(0);
            Map<String, Object> matchInLog;
            try {
                LOG.debug("Looking through {}", firstLog);
                matchInLog = substringSearch(firstLog, targetStr, numMatches - matchCount, startByteOffset);
                scannedFiles++;
            } catch (InvalidRequestException e) {
                LOG.error("Can't search past end of file.", e);
                matchInLog = new HashMap<>();
            }

            String fileName = WorkerLogs.getTopologyPortWorkerLog(firstLog);

            //This section simply put the formatted log filename and corresponding port in the matching.
            final List<Map<String, Object>> newMatches = new ArrayList<>(matches);
            Map<String, Object> currentFileMatch = new HashMap<>(matchInLog);
            currentFileMatch.put("fileName", fileName);
            Path firstLogAbsPath = firstLog.toAbsolutePath().normalize();
            currentFileMatch.put("port", truncatePathToLastElements(firstLogAbsPath, 2).getName(0).toString());
            newMatches.add(currentFileMatch);

            int newCount = matchCount + ((List<?>) matchInLog.getOrDefault("matches", Collections.emptyList())).size();
            if (newCount == matchCount) {
                // matches and matchCount is not changed
                logs = rest(logs);
                startByteOffset = 0;
                fileOffset = fileOffset + 1;
            } else if (newCount >= numMatches) {
                matches = newMatches;
                //fileOffset = the index of last scanned file
                break;
            } else {
                matches = newMatches;
                logs = rest(logs);
                startByteOffset = 0;
                fileOffset = fileOffset + 1;
                matchCount = newCount;
            }
        }

        LOG.debug("scanned {} files", scannedFiles);
        return new Matched(fileOffset, targetStr, matches, scannedFiles);
    }

    /**
     * As the file is read into a buffer, 1/2 the buffer's size at a time, we search the buffer for matches of the substring and return a
     * list of zero or more matches.
     */
    private SubstringSearchResult bufferSubstringSearch(boolean isDaemon, Path file, int fileLength, int offsetToBuf,
        int initBufOffset, BufferedInputStream stream, Integer bytesSkipped,
        int bytesRead, ByteBuffer haystack, byte[] needle,
        List<Map<String, Object>> initialMatches, Integer numMatches, byte[] beforeBytes)
        throws IOException {
        int bufOffset = initBufOffset;
        List<Map<String, Object>> matches = initialMatches;

        byte[] newBeforeBytes;
        Integer newByteOffset;

        while (true) {
            int offset = offsetOfBytes(haystack.array(), needle, bufOffset);
            if (matches.size() < numMatches && offset >= 0) {
                final int fileOffset = offsetToBuf + offset;
                final int bytesNeededAfterMatch = haystack.limit() - GREP_CONTEXT_SIZE - needle.length;

                byte[] beforeArg = null;
                byte[] afterArg = null;
                if (offset < GREP_CONTEXT_SIZE) {
                    beforeArg = beforeBytes;
                }

                if (offset > bytesNeededAfterMatch) {
                    afterArg = tryReadAhead(stream, haystack, offset, fileLength, bytesRead);
                }

                bufOffset = offset + needle.length;
                matches.add(mkMatchData(needle, haystack, offset, fileOffset,
                    file.toAbsolutePath().normalize(), isDaemon, beforeArg, afterArg));
            } else {
                int beforeStrToOffset = Math.min(haystack.limit(), GREP_MAX_SEARCH_SIZE);
                int beforeStrFromOffset = Math.max(0, beforeStrToOffset - GREP_CONTEXT_SIZE);
                newBeforeBytes = Arrays.copyOfRange(haystack.array(), beforeStrFromOffset, beforeStrToOffset);

                // It's OK if new-byte-offset is negative.
                // This is normal if we are out of bytes to read from a small file.
                if (matches.size() >= numMatches) {
                    newByteOffset = ((Number) last(matches).get("byteOffset")).intValue() + needle.length;
                } else {
                    newByteOffset = bytesSkipped + bytesRead - GREP_MAX_SEARCH_SIZE;
                }

                break;
            }
        }

        return new SubstringSearchResult(matches, newByteOffset, newBeforeBytes);
    }

    private int rotateGrepBuffer(ByteBuffer buf, BufferedInputStream stream, int totalBytesRead, int fileLength) throws IOException {
        byte[] bufArray = buf.array();

        // Copy the 2nd half of the buffer to the first half.
        System.arraycopy(bufArray, GREP_MAX_SEARCH_SIZE, bufArray, 0, GREP_MAX_SEARCH_SIZE);

        // Zero-out the 2nd half to prevent accidental matches.
        Arrays.fill(bufArray, GREP_MAX_SEARCH_SIZE, bufArray.length, (byte) 0);

        // Fill the 2nd half with new bytes from the stream.
        int bytesRead = stream.read(bufArray, GREP_MAX_SEARCH_SIZE, Math.min(fileLength, GREP_MAX_SEARCH_SIZE));
        buf.limit(GREP_MAX_SEARCH_SIZE + bytesRead);
        return totalBytesRead + bytesRead;
    }

    private Map<String, Object> mkMatchData(byte[] needle, ByteBuffer haystack, int haystackOffset, int fileOffset, Path canonicalPath,
        boolean isDaemon, byte[] beforeBytes, byte[] afterBytes)
        throws UnsupportedEncodingException, UnknownHostException {
        String url;
        if (isDaemon) {
            url = urlToMatchCenteredInLogPageDaemonFile(needle, canonicalPath, fileOffset, logviewerPort);
        } else {
            url = urlToMatchCenteredInLogPage(needle, canonicalPath, fileOffset, logviewerPort);
        }

        byte[] haystackBytes = haystack.array();
        String beforeString;
        String afterString;

        if (haystackOffset >= GREP_CONTEXT_SIZE) {
            beforeString = new String(haystackBytes, (haystackOffset - GREP_CONTEXT_SIZE), GREP_CONTEXT_SIZE, "UTF-8");
        } else {
            int numDesired = Math.max(0, GREP_CONTEXT_SIZE - haystackOffset);
            int beforeSize = beforeBytes != null ? beforeBytes.length : 0;
            int numExpected = Math.min(beforeSize, numDesired);

            if (numExpected > 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(new String(beforeBytes, beforeSize - numExpected, numExpected, "UTF-8"));
                sb.append(new String(haystackBytes, 0, haystackOffset, "UTF-8"));
                beforeString = sb.toString();
            } else {
                beforeString = new String(haystackBytes, 0, haystackOffset, "UTF-8");
            }
        }

        int needleSize = needle.length;
        int afterOffset = haystackOffset + needleSize;
        int haystackSize = haystack.limit();

        if ((afterOffset + GREP_CONTEXT_SIZE) < haystackSize) {
            afterString = new String(haystackBytes, afterOffset, GREP_CONTEXT_SIZE, "UTF-8");
        } else {
            int numDesired = GREP_CONTEXT_SIZE - (haystackSize - afterOffset);
            int afterSize = afterBytes != null ? afterBytes.length : 0;
            int numExpected = Math.min(afterSize, numDesired);

            if (numExpected > 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(new String(haystackBytes, afterOffset, (haystackSize - afterOffset), "UTF-8"));
                sb.append(new String(afterBytes, 0, numExpected, "UTF-8"));
                afterString = sb.toString();
            } else {
                afterString = new String(haystackBytes, afterOffset, (haystackSize - afterOffset), "UTF-8");
            }
        }

        Map<String, Object> ret = new HashMap<>();
        ret.put("byteOffset", fileOffset);
        ret.put("beforeString", beforeString);
        ret.put("afterString", afterString);
        ret.put("matchString", new String(needle, "UTF-8"));
        ret.put("logviewerURL", url);

        return ret;
    }

    /**
     * Tries once to read ahead in the stream to fill the context and resets the stream to its position before the call.
     */
    private byte[] tryReadAhead(BufferedInputStream stream, ByteBuffer haystack, int offset, int fileLength, int bytesRead)
        throws IOException {
        int numExpected = Math.min(fileLength - bytesRead, GREP_CONTEXT_SIZE);
        byte[] afterBytes = new byte[numExpected];
        stream.mark(numExpected);
        // Only try reading once.
        stream.read(afterBytes, 0, numExpected);
        stream.reset();
        return afterBytes;
    }

    /**
     * Searches a given byte array for a match of a sub-array of bytes. Returns the offset to the byte that matches, or -1 if no match was
     * found.
     */
    private int offsetOfBytes(byte[] buffer, byte[] search, int initOffset) {
        if (search.length <= 0) {
            throw new IllegalArgumentException("Search array should not be empty.");
        }

        if (initOffset < 0) {
            throw new IllegalArgumentException("Start offset shouldn't be negative.");
        }

        int offset = initOffset;
        int candidateOffset = initOffset;
        int valOffset = 0;
        int retOffset = 0;

        while (true) {
            if (search.length - valOffset <= 0) {
                // found
                retOffset = candidateOffset;
                break;
            } else {
                if (offset >= buffer.length) {
                    // We ran out of buffer for the search.
                    retOffset = -1;
                    break;
                } else {
                    if (search[valOffset] != buffer[offset]) {
                        // The match at this candidate offset failed, so start over with the
                        // next candidate byte from the buffer.
                        int newOffset = candidateOffset + 1;

                        offset = newOffset;
                        candidateOffset = newOffset;
                        valOffset = 0;
                    } else {
                        // So far it matches.  Keep going...
                        offset = offset + 1;
                        valOffset = valOffset + 1;
                    }
                }
            }
        }

        return retOffset;
    }

    /**
     * This response data only includes a next byte offset if there is more of the file to read.
     */
    private Map<String, Object> mkGrepResponse(byte[] searchBytes, Integer offset, List<Map<String, Object>> matches,
        Integer nextByteOffset) throws UnsupportedEncodingException {
        Map<String, Object> ret = new HashMap<>();
        ret.put("searchString", new String(searchBytes, "UTF-8"));
        ret.put("startByteOffset", offset);
        ret.put("matches", matches);
        if (nextByteOffset != null) {
            ret.put("nextByteOffset", nextByteOffset);
        }
        return ret;
    }

    @VisibleForTesting
    String urlToMatchCenteredInLogPage(byte[] needle, Path canonicalPath, int offset, Integer port) throws UnknownHostException {
        final String host = Utils.hostname();
        final Path truncatedFilePath = truncatePathToLastElements(canonicalPath, 3);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("file", truncatedFilePath.toString());
        parameters.put("start", Math.max(0, offset - (LogviewerConstant.DEFAULT_BYTES_PER_PAGE / 2) - (needle.length / -2)));
        parameters.put("length", LogviewerConstant.DEFAULT_BYTES_PER_PAGE);

        return UrlBuilder.build(String.format(this.scheme + "://%s:%d/api/v1/log", host, port), parameters);
    }

    @VisibleForTesting
    String urlToMatchCenteredInLogPageDaemonFile(byte[] needle, Path canonicalPath, int offset, Integer port) throws UnknownHostException {
        final String host = Utils.hostname();
        final Path truncatedFilePath = truncatePathToLastElements(canonicalPath, 1);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("file", truncatedFilePath.toString());
        parameters.put("start", Math.max(0, offset - (LogviewerConstant.DEFAULT_BYTES_PER_PAGE / 2) - (needle.length / -2)));
        parameters.put("length", LogviewerConstant.DEFAULT_BYTES_PER_PAGE);

        return UrlBuilder.build(String.format(this.scheme + "://%s:%d/api/v1/daemonlog", host, port), parameters);
    }

    @VisibleForTesting
    public static class Matched implements JSONAware {

        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private int fileOffset;
        private String searchString;
        private List<Map<String, Object>> matches;
        @JsonIgnore
        private final int openedFiles;

        /**
         * Constructor.
         *
         * @param fileOffset offset (index) of the files
         * @param searchString search string
         * @param matches map representing matched search result
         * @param openedFiles number of files scanned, used for metrics only
         */
        public Matched(int fileOffset, String searchString, List<Map<String, Object>> matches, int openedFiles) {
            this.fileOffset = fileOffset;
            this.searchString = searchString;
            this.matches = matches;
            this.openedFiles = openedFiles;
        }

        public int getFileOffset() {
            return fileOffset;
        }

        public String getSearchString() {
            return searchString;
        }

        public List<Map<String, Object>> getMatches() {
            return matches;
        }

        @Override
        public String toJSONString() {
            try {
                return OBJECT_MAPPER.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class SubstringSearchResult {

        private List<Map<String, Object>> matches;
        private Integer newByteOffset;
        private byte[] newBeforeBytes;

        SubstringSearchResult(List<Map<String, Object>> matches, Integer newByteOffset, byte[] newBeforeBytes) {
            this.matches = matches;
            this.newByteOffset = newByteOffset;
            this.newBeforeBytes = newBeforeBytes;
        }

        public List<Map<String, Object>> getMatches() {
            return matches;
        }

        public Integer getNewByteOffset() {
            return newByteOffset;
        }

        public byte[] getNewBeforeBytes() {
            return newBeforeBytes;
        }
    }
}
