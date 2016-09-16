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

package org.apache.storm.scheduler.utils;

import org.apache.storm.utils.Time;
import org.apache.storm.Config;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.config.RequestConfig; 
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder; 
import org.apache.http.client.HttpClient; 
import org.apache.http.util.EntityUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtifactoryConfigLoader implements IConfigLoader {
    /**
     * Configuration items for this config loader are passed in via confg settings in 
     * each scheduler that has a configurable loader.
     *
     * For example, the resource aware scheduler has configuration items defined in Config.java
     * that allow a user to configure which implementation of IConfigLoader to use to load
     * specific scheduler configs as well as any parameters to pass into the prepare method of
     * tht configuration.
     *
     * resource.aware.scheduler.user.pools.loader can be set to org.apache.storm.scheduler.utils.ArtifactoryConfigLoader
     *
     * and then
     *
     * resource.aware.scheduler.user.pools.loader.params can be set to any of the following
     *
     *  {"artifactory.config.loader.uri": "http://artifactory.example.org:9989/artifactory/confs/my_cluster/mt_user_pool"}
     *
     *  {"artifactory.config.loader.uri": "file:///confs/my_cluster/mt_user_pool"}
     *
     *  {"artifactory.config.loader.uri": "file:///confs/my_cluster/mt_user_pool", "artifactory.config.loader.timeout.secs" : "60"}
     **/
    protected static final String ARTIFACTORY_URI = "artifactory.config.loader.uri";
    protected static final String ARTIFACTORY_TIMEOUT_SECS="artifactory.config.loader.timeout.secs";
    protected static final String ARTIFACTORY_POLL_TIME_SECS="artifactory.config.loader.polltime.secs";
    protected static final String ARTIFACTORY_SCHEME="artifactory.config.loader.scheme";
    protected static final String ARTIFACTORY_BASE_DIRECTORY="artifactory.config.loader.base.directory";
    protected static final String LOCAL_ARTIFACT_DIR="scheduler_artifacts";
    static final String cacheFilename = "latest.yaml";

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactoryConfigLoader.class);

    @SuppressWarnings("rawtypes")
    private Map _conf;
    private int _artifactoryPollTimeSecs = 600;
    private boolean _cacheInitialized = false;
    // Location of the file in the artifactory archive.  Also used to name file in cache.
    private String _localCacheDir;
    private String _artifactoryScheme = "http";
    private String _baseDirectory = "/artifactory";
    private int _lastReturnedTime = 0;
    private int _timeoutSeconds = 10;
    private Map _lastReturnedValue;

    private static class OurResponseHandler implements ResponseHandler<String> {
        private static OurResponseHandler singleton = null;

        public static OurResponseHandler getInstance() {
            if (singleton == null) {
                singleton = new OurResponseHandler();
            }
            return singleton;
        }

        @Override
        public String handleResponse(final HttpResponse response) throws IOException {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                HttpEntity entity = response.getEntity();
                return entity != null ? EntityUtils.toString(entity) : null;
            } else {
                LOG.error("Got unexpected response code {}", status);
                return null;
            }
        }
    };

    // Protected so we can override this in testing
    protected String doGet(String api, String artifact, String host, Integer port) {
        URIBuilder builder = new URIBuilder().setScheme(_artifactoryScheme).setHost(host).setPort(port);

        if (api != null) {
            builder.setPath(_baseDirectory + api + artifact);
        } else {
            builder.setPath(_baseDirectory + artifact);
        }
        
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(_timeoutSeconds * 1000).build();
        HttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        String returnValue;
        try {
            LOG.debug("About to issue a GET to {}", builder);
            HttpGet httpget = new HttpGet(builder.build());

            String responseBody;
            responseBody = httpclient.execute(httpget, OurResponseHandler.getInstance());
            returnValue = responseBody;
        } catch (Exception e) {
            LOG.error("Received exception while connecting to Artifactory", e);
            returnValue=null;
        }

        LOG.debug("Returning {}",returnValue);
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
            returnValue = (JSONObject) new JSONParser().parse(metadataStr);
        } catch (ParseException e) {
            LOG.error("Could not parse JSON string {}", metadataStr, e);
            return null;
        }

        return returnValue;
    }

    private class DirEntryCompare implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject o1, JSONObject o2) {
            return ((String)o1.get("uri")).compareTo((String)o2.get("uri"));
        }
    }

    private String loadMostRecentArtifact(String location, String host, Integer port) {
        // Is this a directory or is it a file?
        JSONObject json = getArtifactMetadata(location, host, port);
        if (json == null) {
            LOG.error("got null metadata");
            return null;
        }
        String downloadURI = (String) json.get("downloadUri");

        // This means we are pointing at a file.
        if (downloadURI != null) {
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

    private Map loadFromFile(File file) {
        Map ret = SchedulerUtils.loadYamlFromFile(file);

        if (ret != null) {
            try {
                LOG.debug("returning a new map from file {}", file.getCanonicalPath());
            } catch (java.io.IOException e) {
                LOG.debug("Could not get PATH from file object in debug print. Ignoring");
            }
            _lastReturnedTime = Time.currentTimeSecs();
            _lastReturnedValue = ret;
            return _lastReturnedValue;
        }

        return null;
    }


    private Map getLatestFromCache() {
        String localFileName = _localCacheDir + File.separator + cacheFilename;
        return loadFromFile(new File(localFileName));
    }

    private void saveInArtifactoryCache(String yamlData) {
        String localFileName = _localCacheDir + File.separator + cacheFilename;

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

    private void makeArtifactoryCache(String location) {
        // Just make sure approprite directories exist
        String localDirName = (String)_conf.get(Config.STORM_LOCAL_DIR); 
        if (localDirName == null) {
            return;
        }

        // First make the cache dir
        localDirName = localDirName + File.separator + "nimbus" + File.separator + LOCAL_ARTIFACT_DIR;
        File dir = new File(localDirName);
        if (! dir.exists()) {
            dir.mkdir();
        }

        _localCacheDir = localDirName + File.separator + location.replaceAll(File.separator, "_");
        dir = new File(_localCacheDir);
        if (! dir.exists()) {
            dir.mkdir();
        }
        _cacheInitialized = true;
    }

    @Override
    public void prepare(Map conf) {
        _conf = conf;
        String thisTimeout = (String)_conf.get(ARTIFACTORY_TIMEOUT_SECS);
        if (thisTimeout != null) {
            _timeoutSeconds = Integer.parseInt(thisTimeout);
        }
        String thisPollTime = (String)_conf.get(ARTIFACTORY_POLL_TIME_SECS);
        if (thisPollTime != null) {
            _artifactoryPollTimeSecs = Integer.parseInt(thisPollTime);
        }
        String thisScheme = (String)_conf.get(ARTIFACTORY_SCHEME);
        if (thisScheme != null) {
            _artifactoryScheme = thisScheme;
        }
        String thisBase = (String)_conf.get(ARTIFACTORY_BASE_DIRECTORY);
        if (thisBase != null) {
            _baseDirectory = thisBase;
        }
    }

    @Override
    public Map load() {
        // Check for new file every so often 
        if (_lastReturnedValue != null && ((Time.currentTimeSecs() - _lastReturnedTime) < _artifactoryPollTimeSecs)) {
            LOG.debug("returning our last map");
            return _lastReturnedValue;
        }

        String myScheme = null;
        String location = null;
        String host = null;
        Integer port = null;
        String uriString = (String)_conf.get(ARTIFACTORY_URI);
        String filePath = null;

        if (uriString != null) {
            URI uri = null;
            try {
                uri = new URI(uriString);
                myScheme = uri.getScheme().toLowerCase();
                if (myScheme.equals("http")) {
                    host = uri.getHost();
                    port = uri.getPort();
                    location = uri.getPath();
                    if (location.toLowerCase().startsWith(_baseDirectory.toLowerCase())) {
                        location = location.substring(_baseDirectory.length());
                    }
                } else if (myScheme.equals("file")) {
                    filePath = uri.getPath();
                }
            } catch (java.net.URISyntaxException e) {
                LOG.error("Failed to parse uri={}", uriString);
                return null;
            }
        } else {
            LOG.error("URI is null");
            return null;
        }

        // host, port, location are only non-null if uri's scheme is set to 
        // http.  If uri's scheme is "file" then these members will be null
        // If urs has a file scheme, filePath will be non-null.  We should not
        // be in a state where filePath is non null, while host, port, and location
        // are also non-null
        if (myScheme.equals("http")) {

            if (!_cacheInitialized) {
                makeArtifactoryCache(location);
            }

            // Get the most recent artifact as a String, and then parse the yaml
            String yamlConfig = loadMostRecentArtifact(location, host, port);

            // If we failed to get anything from Artifactory try to get it from our local cache
            if (yamlConfig == null) {
                return getLatestFromCache();
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
                _lastReturnedTime = Time.currentTimeSecs();
                _lastReturnedValue = ret;
                return _lastReturnedValue;
            }
        } else if (myScheme.equals("file")) {
            File file = new File(filePath);
            Map ret = loadFromFile(file);

            if (ret != null) {
                LOG.debug("returning a new map from file {}", filePath);
                _lastReturnedTime = Time.currentTimeSecs();
                _lastReturnedValue = ret;
                return _lastReturnedValue;
            }
        } else {
            LOG.error("Unhandled scheme {}", myScheme);
        }

        return null;
    }
}
