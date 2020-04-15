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

package org.apache.storm.scheduler.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * A dynamic loader that can load scheduler configurations for user resource guarantees from Artifactory (an artifact repository manager).
 * This is not thread-safe.
 */
public class ArtifactoryConfigLoader implements IConfigLoader {
    protected static final String LOCAL_ARTIFACT_DIR = "scheduler_artifacts";
    static final String cacheFilename = "latest.yaml";
    private static final String DEFAULT_ARTIFACTORY_BASE_DIRECTORY = "/artifactory";
    private static final int DEFAULT_POLLTIME_SECS = 600;
    private static final int DEFAULT_TIMEOUT_SECS = 10;
    private static final String ARTIFACTORY_SCHEME_PREFIX = "artifactory+";

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactoryConfigLoader.class);

    private Map<String, Object> conf;
    private int artifactoryPollTimeSecs = DEFAULT_POLLTIME_SECS;
    private boolean cacheInitialized = false;
    // Location of the file in the artifactory archive.  Also used to name file in cache.
    private String localCacheDir;
    private String baseDirectory = DEFAULT_ARTIFACTORY_BASE_DIRECTORY;
    private int lastReturnedTime = 0;
    private int timeoutSeconds = DEFAULT_TIMEOUT_SECS;
    private Map<String, Object> lastReturnedValue;
    private URI targetUri = null;
    private JSONParser jsonParser;
    private String scheme;

    public ArtifactoryConfigLoader(Map<String, Object> conf) {
        this.conf = conf;
        Integer thisTimeout = (Integer) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_TIMEOUT_SECS);
        if (thisTimeout != null) {
            timeoutSeconds = thisTimeout;
        }
        Integer thisPollTime = (Integer) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_POLLTIME_SECS);
        if (thisPollTime != null) {
            artifactoryPollTimeSecs = thisPollTime;
        }
        String thisBase = (String) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_ARTIFACTORY_BASE_DIRECTORY);
        if (thisBase != null) {
            baseDirectory = thisBase;
        }
        String uriString = (String) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        if (uriString == null) {
            LOG.error("No URI defined in {} configuration.", DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        } else {
            try {
                targetUri = new URI(uriString);
                scheme = targetUri.getScheme().substring(ARTIFACTORY_SCHEME_PREFIX.length());
            } catch (URISyntaxException e) {
                LOG.error("Failed to parse uri={}", uriString);
            }
        }
        jsonParser = new JSONParser();
    }

    /**
     * Load the configs associated with the configKey from the targetURI.
     * @param configKey The key from which we want to get the scheduler config.
     * @return The scheduler configuration if exists; null otherwise.
     */
    @Override
    public Map<String, Object> load(String configKey) {
        if (targetUri == null) {
            return null;
        }

        // Check for new file every so often
        int currentTimeSecs = Time.currentTimeSecs();
        if (lastReturnedValue != null && ((currentTimeSecs - lastReturnedTime) < artifactoryPollTimeSecs)) {
            LOG.debug("currentTimeSecs: {}; lastReturnedTime {}; artifactoryPollTimeSecs: {}. Returning our last map.",
                      currentTimeSecs, lastReturnedTime, artifactoryPollTimeSecs);
            return (Map<String, Object>) lastReturnedValue.get(configKey);
        }

        try {
            Map<String, Object> raw = loadFromUri(targetUri);
            if (raw != null) {
                return (Map<String, Object>) raw.get(configKey);
            }
        } catch (Exception e) {
            LOG.error("Failed to load from uri {}", targetUri);
        }
        return null;
    }

    /**
     * Protected so we can override this in unit tests.
     *
     * @param api null if we are trying to download artifact, otherwise a string to call REST api,
     *        e.g. "/api/storage"
     * @param artifact location of artifact
     * @param host Artifactory hostname
     * @param port Artifactory port
     * @return null on failure or the response string if return code is in 200 range
     */
    protected String doGet(String api, String artifact, String host, Integer port) {
        URIBuilder builder = new URIBuilder().setScheme(scheme).setHost(host).setPort(port);

        String path = null;
        if (api != null) {
            path = baseDirectory + "/" + api + "/" + artifact;
        } else {
            path = baseDirectory + "/" + artifact;
        }

        // Get rid of multiple '/' in url
        path = path.replaceAll("/[/]+", "/");
        builder.setPath(path);

        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeoutSeconds * 1000).build();
        HttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        String returnValue;
        try {
            LOG.debug("About to issue a GET to {}", builder);
            HttpGet httpget = new HttpGet(builder.build());
            String responseBody;
            responseBody = httpclient.execute(httpget, GetStringResponseHandler.getInstance());
            returnValue = responseBody;
        } catch (Exception e) {
            LOG.error("Received exception while connecting to Artifactory", e);
            returnValue = null;
        }

        LOG.debug("Returning {}", returnValue);
        return returnValue;
    }

    private JSONObject getArtifactMetadata(String location, String host, Integer port) {
        String metadataStr = null;

        metadataStr = doGet("/api/storage", location, host, port);

        if (metadataStr == null) {
            return null;
        }

        JSONObject returnValue;
        try {
            returnValue = (JSONObject) jsonParser.parse(metadataStr);
        } catch (ParseException e) {
            LOG.error("Could not parse JSON string {}", metadataStr, e);
            return null;
        }

        return returnValue;
    }

    private String loadMostRecentArtifact(String location, String host, Integer port) {
        // Is this a directory or is it a file?
        JSONObject json = getArtifactMetadata(location, host, port);
        if (json == null) {
            LOG.error("got null metadata");
            return null;
        }
        String downloadUri = (String) json.get("downloadUri");

        // This means we are pointing at a file.
        if (downloadUri != null) {
            // Then get it and return the file as string.
            String returnValue = doGet(null, location, host, port);
            saveInArtifactoryCache(returnValue);
            return returnValue;
        }

        // This should mean that we were pointed at a directory.
        // Find the most recent child and load that.
        JSONArray msg = (JSONArray) json.get("children");
        if (msg == null || msg.size() == 0) {
            LOG.error("Expected directory children not present");
            return null;
        }
        JSONObject newest = (JSONObject) Collections.max(msg, new DirEntryCompare());
        if (newest == null) {
            LOG.error("Failed to find most recent artifact uri in {}", location);
            return null;
        }

        String uri = (String) newest.get("uri");
        if (uri == null) {
            LOG.error("Expected directory uri not present");
            return null;
        }
        String returnValue = doGet(null, location + uri, host, port);
        saveInArtifactoryCache(returnValue);
        return returnValue;
    }

    private void updateLastReturned(Map ret) {
        lastReturnedTime = Time.currentTimeSecs();
        lastReturnedValue = ret;
    }

    private Map<String, Object> loadFromFile(File file) {
        Map<String, Object> ret = null;

        try {
            ret = (Map<String, Object>) Utils.readYamlFile(file.getCanonicalPath());
        } catch (IOException e) {
            LOG.error("Filed to load from file. Exception: {}", e.getMessage());
        }

        if (ret != null) {
            try {
                LOG.debug("returning a new map from file {}", file.getCanonicalPath());
            } catch (java.io.IOException e) {
                LOG.debug("Could not get PATH from file object in debug print. Ignoring");
            }
            return ret;
        }

        return null;
    }

    private Map<String, Object> getLatestFromCache() {
        String localFileName = localCacheDir + File.separator + cacheFilename;
        return loadFromFile(new File(localFileName));
    }

    private void saveInArtifactoryCache(String yamlData) {
        if (yamlData == null) {
            LOG.warn("Will not save null data into the artifactory cache");
            return;
        }

        String localFileName = localCacheDir + File.separator + cacheFilename;

        File cacheFile = new File(localFileName);
        try (FileOutputStream fos = new FileOutputStream(cacheFile)) {
            fos.write(yamlData.getBytes());
            fos.flush();
        } catch (IOException e) {
            LOG.error("Received exception when writing file {}.  Attempting delete", localFileName, e);
            try {
                cacheFile.delete();
            } catch (Exception deleteException) {
                LOG.error("Received exception when deleting file {}.", localFileName, deleteException);
            }
        }
    }

    private void makeArtifactoryCache(String location) throws IOException {
        // First make the cache dir
        String localDirName = ServerConfigUtils.masterLocalDir(conf) + File.separator + LOCAL_ARTIFACT_DIR;
        File dir = new File(localDirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        localCacheDir = localDirName + File.separator + location.replaceAll(File.separator, "_");
        dir = new File(localCacheDir);
        if (!dir.exists()) {
            dir.mkdir();
        }
        cacheInitialized = true;
    }

    private Map<String, Object> loadFromUri(URI uri) throws IOException {
        String host = uri.getHost();
        Integer port = uri.getPort();
        String location = uri.getPath();
        if (location.toLowerCase().startsWith(baseDirectory.toLowerCase())) {
            location = location.substring(baseDirectory.length());
        }

        if (!cacheInitialized) {
            makeArtifactoryCache(location);
        }

        // Get the most recent artifact as a String, and then parse the yaml
        String yamlConfig = loadMostRecentArtifact(location, host, port);

        // If we failed to get anything from Artifactory try to get it from our local cache
        if (yamlConfig == null) {
            Map<String, Object> ret = getLatestFromCache();
            updateLastReturned(ret);
            return ret;
        }

        // Now parse it and return the map.
        Yaml yaml = new Yaml(new SafeConstructor());
        Map ret = null;
        try {
            ret = (Map) yaml.load(yamlConfig);
        } catch (Exception e) {
            LOG.error("Could not parse yaml.");
            return null;
        }

        if (ret != null) {
            LOG.debug("returning a new map from Artifactory");
            updateLastReturned(ret);
            return ret;
        }

        return null;
    }

    /**
     * A private class used to check the response coming back from httpclient.
     */
    private static class GetStringResponseHandler implements ResponseHandler<String> {
        private static GetStringResponseHandler singleton = null;

        /**
         * Get instance.
         * @return a singleton httpclient GET response handler
         */
        public static GetStringResponseHandler getInstance() {
            if (singleton == null) {
                singleton = new GetStringResponseHandler();
            }
            return singleton;
        }

        /**
         * Handle response.
         * @param response The http response to verify.
         * @return null on failure or the response string if return code is in 200 range
         */
        @Override
        public String handleResponse(final HttpResponse response) throws IOException {
            int status = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String entityString = (entity != null ? EntityUtils.toString(entity) : null);
            if (status >= 200 && status < 300) {
                return entityString;
            } else {
                LOG.error("Got unexpected response code {}; entity: {}", status, entityString);
                return null;
            }
        }
    }

    private class DirEntryCompare implements Comparator<JSONObject> {

        @Override
        public int compare(JSONObject o1, JSONObject o2) {
            return ((String) o1.get("uri")).compareTo((String) o2.get("uri"));
        }
    }
}