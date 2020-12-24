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

package org.apache.storm;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.dependency.DependencyPropertiesParser;
import org.apache.storm.dependency.DependencyUploader;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.hooks.SubmitterHookException;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.BufferFileInputStream;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedInvalidTopologyException;
import org.apache.storm.validation.ConfigValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program with the "storm jar" command from the
 * command-line, and then use this class to submit your topologies.
 */
public class StormSubmitter {
    public static final Logger LOG = LoggerFactory.getLogger(StormSubmitter.class);
    public static final Pattern zkDigestPattern = Pattern.compile("\\S+:\\S+");
    private static final int THRIFT_CHUNK_SIZE_BYTES = 307200;

    private static String generateZookeeperDigestSecretPayload() {
        return Utils.secureRandomLong() + ":" + Utils.secureRandomLong();
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static boolean validateZKDigestPayload(String payload) {
        if (payload != null) {
            Matcher m = zkDigestPattern.matcher(payload);
            return m.matches();
        }
        return false;
    }

    public static Map<String, Object> prepareZookeeperAuthentication(Map<String, Object> conf) {
        Map<String, Object> toRet = new HashMap<>();
        String secretPayload = (String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        // Is the topology ZooKeeper authentication configuration unset?
        if (!conf.containsKey(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD)
                || conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD) == null
                || !validateZKDigestPayload((String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD))) {
            secretPayload = generateZookeeperDigestSecretPayload();
            LOG.info("Generated ZooKeeper secret payload for MD5-digest: " + secretPayload);
        }
        toRet.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, secretPayload);
        // This should always be set to digest.
        toRet.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");
        return toRet;
    }

    private static Map<String, String> populateCredentials(Map<String, Object> conf, Map<String, String> creds) {
        Map<String, String> ret = new HashMap<>();
        for (IAutoCredentials autoCred : ClientAuthUtils.getAutoCredentials(conf)) {
            LOG.info("Running " + autoCred);
            autoCred.populateCredentials(ret);
        }
        if (creds != null) {
            ret.putAll(creds);
        }
        return ret;
    }

    /**
     * Push a new set of credentials to the running topology.
     *
     * @param name        the name of the topology to push credentials to.
     * @param topoConf    the topology-specific configuration, if desired. See {@link Config}.
     * @param credentials the credentials to push.
     * @return whether the pushed credential collection is non-empty. Return false if empty.
     * @throws AuthorizationException   if you are not authorized ot push credentials.
     * @throws NotAliveException        if the topology is not alive
     * @throws InvalidTopologyException if any other error happens
     */
    public static boolean pushCredentials(String name, Map<String, Object> topoConf, Map<String, String> credentials)
        throws AuthorizationException, NotAliveException, InvalidTopologyException {
        return pushCredentials(name, topoConf, credentials, null);
    }

    /**
     * Push a new set of credentials to the running topology.
     * Return false if push Creds map is empty, true otherwise.
     *
     * @param name        the name of the topology to push credentials to.
     * @param topoConf    the topology-specific configuration, if desired. See {@link Config}.
     * @param credentials the credentials to push.
     * @param expectedUser the user you expect the topology to be owned by.
     * @return whether the pushed credential collection is non-empty. Return false if empty.
     * @throws AuthorizationException   if you are not authorized ot push credentials.
     * @throws NotAliveException        if the topology is not alive
     * @throws InvalidTopologyException if any other error happens
     */
    public static boolean pushCredentials(String name, Map<String, Object> topoConf, Map<String, String> credentials, String expectedUser)
        throws AuthorizationException, NotAliveException, InvalidTopologyException {
        topoConf = new HashMap<>(topoConf);
        topoConf.putAll(Utils.readCommandLineOpts());
        Map<String, Object> conf = Utils.readStormConfig();
        conf.putAll(topoConf);
        Map<String, String> fullCreds = populateCredentials(conf, credentials);
        if (fullCreds.isEmpty()) {
            LOG.warn("No credentials were found to push to " + name);
            return false;
        }
        try {
            try (NimbusClient client = NimbusClient.getConfiguredClient(conf)) {
                LOG.info("Uploading new credentials to {}", name);
                Credentials creds = new Credentials(fullCreds);
                if (expectedUser != null) {
                    creds.set_topoOwner(expectedUser);
                }
                client.getClient().uploadNewCredentials(name, creds);
            }
            LOG.info("Finished pushing creds to topology: {}", name);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        return true;
    }


    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     * @param name     the name of the storm.
     * @param topoConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException   if authorization is failed
     * @throws SubmitterHookException if any Exception occurs during initialization or invocation of registered {@link ISubmitterHook}
     */
    public static void submitTopology(String name, Map<String, Object> topoConf, StormTopology topology)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopology(name, topoConf, topology, null, null);
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     * @param name     the name of the storm.
     * @param topoConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts     to manipulate the starting of the topology.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException   if authorization is failed
     * @throws SubmitterHookException if any Exception occurs during initialization or invocation of registered {@link ISubmitterHook}
     */
    public static void submitTopology(String name, Map<String, Object> topoConf, StormTopology topology, SubmitOptions opts)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopology(name, topoConf, topology, opts, null);
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     * @param name             the name of the storm.
     * @param topoConf         the topology-specific configuration. See {@link Config}.
     * @param topology         the processing to execute.
     * @param opts             to manipulate the starting of the topology
     * @param progressListener to track the progress of the jar upload process {@link ProgressListener}
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException   if authorization is failed
     * @throws SubmitterHookException if any Exception occurs during initialization or invocation of registered {@link ISubmitterHook}
     */
    @SuppressWarnings("unchecked")
    public static void submitTopology(String name, Map<String, Object> topoConf, StormTopology topology, SubmitOptions opts,
            ProgressListener progressListener) throws AlreadyAliveException, InvalidTopologyException,
            AuthorizationException {
        submitTopologyAs(name, topoConf, topology, opts, progressListener, null);
    }

    /**
     * Submits a topology to run on the cluster as a particular user. A topology runs forever or until explicitly killed.
     *
     * @param asUser The user as which this topology should be submitted.
     * @throws IllegalArgumentException thrown if configs will yield an unschedulable topology. validateConfs validates confs
     * @throws SubmitterHookException if any Exception occurs during initialization or invocation of registered {@link ISubmitterHook}
     */
    public static void submitTopologyAs(String name, Map<String, Object> topoConf, StormTopology topology, SubmitOptions opts,
                                        ProgressListener progressListener, String asUser)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, IllegalArgumentException {

        //validate topology name first; nothing else should be done if it's invalid.
        Utils.validateTopologyName(name);

        if (!Utils.isValidConf(topoConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }

        if (topology.get_spouts_size() == 0) {
            throw new WrappedInvalidTopologyException("Topology " + name + " does not have any spout");
        }

        topoConf = new HashMap<>(topoConf);
        topoConf.putAll(Utils.readCommandLineOpts());
        Map<String, Object> conf = Utils.readStormConfig();
        conf.putAll(topoConf);
        topoConf.putAll(prepareZookeeperAuthentication(conf));

        validateConfs(conf);

        try {
            Utils.validateCycleFree(topology, name);
        } catch (InvalidTopologyException ex) {
            LOG.warn("", ex);
        }

        Map<String, String> passedCreds = new HashMap<>();
        if (opts != null) {
            Credentials tmpCreds = opts.get_creds();
            if (tmpCreds != null) {
                passedCreds = tmpCreds.get_creds();
            }
        }
        Map<String, String> fullCreds = populateCredentials(conf, passedCreds);
        if (!fullCreds.isEmpty()) {
            if (opts == null) {
                opts = new SubmitOptions(TopologyInitialStatus.ACTIVE);
            }
            opts.set_creds(new Credentials(fullCreds));
        }
        try {
            String serConf = JSONValue.toJSONString(topoConf);
            try (NimbusClient client = NimbusClient.getConfiguredClientAs(conf, asUser)) {
                if (!isTopologyNameAllowed(name, client)) {
                    throw new RuntimeException("Topology name " + name + " is either not allowed or it already exists on the cluster");
                }

                // Dependency uploading only makes sense for distributed mode
                List<String> jarsBlobKeys = Collections.emptyList();
                List<String> artifactsBlobKeys;

                DependencyUploader uploader = new DependencyUploader();
                try {
                    uploader.init();

                    jarsBlobKeys = uploadDependencyJarsToBlobStore(uploader);

                    artifactsBlobKeys = uploadDependencyArtifactsToBlobStore(uploader);
                } catch (Throwable e) {
                    // remove uploaded jars blobs, not artifacts since they're shared across the cluster
                    uploader.deleteBlobs(jarsBlobKeys);
                    uploader.shutdown();
                    throw e;
                }

                try {
                    setDependencyBlobsToTopology(topology, jarsBlobKeys, artifactsBlobKeys);
                    submitTopologyInDistributeMode(name, topology, opts, progressListener, asUser, conf, serConf, client);
                } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                    // remove uploaded jars blobs, not artifacts since they're shared across the cluster
                    // Note that we don't handle TException to delete jars blobs
                    // because it's safer to leave some blobs instead of topology not running
                    uploader.deleteBlobs(jarsBlobKeys);
                    throw e;
                } finally {
                    uploader.shutdown();
                }
            }
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        invokeSubmitterHook(name, asUser, conf, topology);

    }

    private static List<String> uploadDependencyJarsToBlobStore(DependencyUploader uploader) {
        LOG.info("Uploading dependencies - jars...");

        DependencyPropertiesParser propertiesParser = new DependencyPropertiesParser();

        String depJarsProp = System.getProperty("storm.dependency.jars", "");
        List<File> depJars = propertiesParser.parseJarsProperties(depJarsProp);

        try {
            return uploader.uploadFiles(depJars, true);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> uploadDependencyArtifactsToBlobStore(DependencyUploader uploader) {
        LOG.info("Uploading dependencies - artifacts...");

        DependencyPropertiesParser propertiesParser = new DependencyPropertiesParser();

        String depArtifactsProp = System.getProperty("storm.dependency.artifacts", "{}");
        Map<String, File> depArtifacts = propertiesParser.parseArtifactsProperties(depArtifactsProp);

        try {
            return uploader.uploadArtifacts(depArtifacts);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void setDependencyBlobsToTopology(StormTopology topology, List<String> jarsBlobKeys, List<String> artifactsBlobKeys) {
        LOG.info("Dependency Blob keys - jars : {} / artifacts : {}", jarsBlobKeys, artifactsBlobKeys);
        topology.set_dependency_jars(jarsBlobKeys);
        topology.set_dependency_artifacts(artifactsBlobKeys);
    }

    private static void submitTopologyInDistributeMode(String name, StormTopology topology, SubmitOptions opts,
                                                       ProgressListener progressListener, String asUser, Map<String, Object> conf,
                                                       String serConf, NimbusClient client) throws TException {
        try {
            String jar = submitJarAs(conf, System.getProperty("storm.jar"), progressListener, client);
            LOG.info("Submitting topology {} in distributed mode with conf {}", name, serConf);
            Utils.addVersions(topology);
            if (opts != null) {
                client.getClient().submitTopologyWithOpts(name, jar, serConf, topology, opts);
            } else {
                // this is for backwards compatibility
                client.getClient().submitTopology(name, jar, serConf, topology);
            }
            LOG.info("Finished submitting topology: {}", name);
        } catch (InvalidTopologyException e) {
            LOG.error("Topology submission exception: {}", e.get_msg());
            throw e;
        } catch (AlreadyAliveException e) {
            LOG.error("Topology already alive exception", e);
            throw e;
        }
    }

    /**
     * Invoke submitter hook.
     * @throws SubmitterHookException This is thrown when any Exception occurs during initialization or invocation of registered {@link
     *     ISubmitterHook}
     */
    private static void invokeSubmitterHook(String name, String asUser, Map<String, Object> topoConf, StormTopology topology) {
        String submissionNotifierClassName = null;
        try {
            if (topoConf.containsKey(Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN)) {
                submissionNotifierClassName = topoConf.get(Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN).toString();
                LOG.info("Initializing the registered ISubmitterHook [{}]", submissionNotifierClassName);

                if (submissionNotifierClassName == null || submissionNotifierClassName.isEmpty()) {
                    throw new IllegalArgumentException(
                        Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN + " property must be a non empty string.");
                }

                ISubmitterHook submitterHook = (ISubmitterHook) Class.forName(submissionNotifierClassName).newInstance();
                TopologyInfo topologyInfo = Utils.getTopologyInfo(name, asUser, topoConf);
                LOG.info("Invoking the registered ISubmitterHook [{}]", submissionNotifierClassName);
                submitterHook.notify(topologyInfo, topoConf, topology);
            }
        } catch (Exception e) {
            LOG.warn("Error occurred in invoking submitter hook:[{}] ", submissionNotifierClassName, e);
            throw new SubmitterHookException(e);
        }
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     *
     * @param name     the name of the storm.
     * @param topoConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException   if authorization is failed
     */

    public static void submitTopologyWithProgressBar(String name, Map<String, Object> topoConf, StormTopology topology) throws
        AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        submitTopologyWithProgressBar(name, topoConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until explicitly killed.
     *
     * @param name     the name of the storm.
     * @param topoConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts     to manipulate the starting of the topology
     * @throws AlreadyAliveException    if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     * @throws AuthorizationException   if authorization is failed
     * @throws SubmitterHookException if any Exception occurs during initialization or invocation of registered {@link ISubmitterHook}
     */
    public static void submitTopologyWithProgressBar(String name, Map<String, Object> topoConf, StormTopology topology,
                                                     SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException,
        AuthorizationException {
        // show a progress bar so we know we're not stuck (especially on slow connections)
        submitTopology(name, topoConf, topology, opts, new StormSubmitter.ProgressListener() {
            @Override
            public void onStart(String srcFile, String targetFile, long totalBytes) {
                System.out.printf("Start uploading file '%s' to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }

            @Override
            public void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes) {
                int length = 50;
                int p = (int) ((length * bytesUploaded) / totalBytes);
                String progress = StringUtils.repeat("=", p);
                String todo = StringUtils.repeat(" ", length - p);

                System.out.printf("\r[%s%s] %d / %d", progress, todo, bytesUploaded, totalBytes);
            }

            @Override
            public void onCompleted(String srcFile, String targetFile, long totalBytes) {
                System.out.printf("\nFile '%s' uploaded to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }
        });
    }

    private static boolean isTopologyNameAllowed(String name, NimbusClient client) {
        try {
            return client.getClient().isTopologyNameAllowed(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Submit jar file.
     *
     * @param conf     the topology-specific configuration. See {@link Config}.
     * @param localJar file path of the jar file to submit
     * @return the remote location of the submitted jar
     */
    public static String submitJar(Map<String, Object> conf, String localJar) {
        return submitJar(conf, localJar, null);
    }

    /**
     * Submit jar file.
     *
     * @param conf     the topology-specific configuration. See {@link Config}.
     * @param localJar file path of the jar file to submit
     * @param listener progress listener to track the jar file upload
     * @return the remote location of the submitted jar
     */
    public static String submitJar(Map<String, Object> conf, String localJar, ProgressListener listener) {
        return submitJarAs(conf, localJar, listener, (String) null);
    }

    public static String submitJarAs(Map<String, Object> conf, String localJar, ProgressListener listener, NimbusClient client) {
        if (localJar == null) {
            throw new RuntimeException(
                "Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }

        try {
            String uploadLocation = client.getClient().beginFileUpload();
            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            BufferFileInputStream is = new BufferFileInputStream(localJar, THRIFT_CHUNK_SIZE_BYTES);

            long totalSize = new File(localJar).length();
            if (listener != null) {
                listener.onStart(localJar, uploadLocation, totalSize);
            }

            long bytesUploaded = 0;
            while (true) {
                byte[] toSubmit = is.read();
                bytesUploaded += toSubmit.length;
                if (listener != null) {
                    listener.onProgress(localJar, uploadLocation, bytesUploaded, totalSize);
                }

                if (toSubmit.length == 0) {
                    break;
                }
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);

            if (listener != null) {
                listener.onCompleted(localJar, uploadLocation, totalSize);
            }

            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String submitJarAs(Map<String, Object> conf, String localJar, ProgressListener listener, String asUser) {
        if (localJar == null) {
            throw new RuntimeException(
                "Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }

        try (NimbusClient client = NimbusClient.getConfiguredClientAs(conf, asUser)) {
            return submitJarAs(conf, localJar, listener, client);
        }
    }

    private static void validateConfs(Map<String, Object> topoConf) throws IllegalArgumentException,
        InvalidTopologyException, AuthorizationException {
        ConfigValidation.validateTopoConf(topoConf);
        Utils.validateTopologyBlobStoreMap(topoConf);
    }

    /**
     * Interface use to track progress of file upload.
     */
    public interface ProgressListener {
        /**
         * called before file is uploaded.
         *
         * @param srcFile    - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        void onStart(String srcFile, String targetFile, long totalBytes);

        /**
         * called whenever a chunk of bytes is uploaded.
         *
         * @param srcFile       - jar file to be uploaded
         * @param targetFile    - destination file
         * @param bytesUploaded - number of bytes transferred so far
         * @param totalBytes    - total number of bytes of the file
         */
        void onProgress(String srcFile, String targetFile, long bytesUploaded, long totalBytes);

        /**
         * called when the file is uploaded.
         *
         * @param srcFile    - jar file to be uploaded
         * @param targetFile - destination file
         * @param totalBytes - total number of bytes of the file
         */
        void onCompleted(String srcFile, String targetFile, long totalBytes);
    }
}
