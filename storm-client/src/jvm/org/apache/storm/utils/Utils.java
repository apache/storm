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

package org.apache.storm.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.NimbusBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.serialization.SerializationDelegate;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.MapDifference;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.shade.org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";
    private static final Set<Class<?>> defaultAllowedExceptions = Collections.emptySet();
    private static final List<String> LOCALHOST_ADDRESSES = Lists.newArrayList("localhost", "127.0.0.1", "0:0:0:0:0:0:0:1");
    static SerializationDelegate serializationDelegate;
    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();
    private static ClassLoader cl = null;
    private static Map<String, Object> localConf;
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static Utils _instance = new Utils();
    private static String memoizedLocalHostnameString = null;
    public static final Pattern BLOB_KEY_PATTERN =
            Pattern.compile("^[\\w \\t\\._-]+$", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern TOPOLOGY_NAME_REGEX = Pattern.compile("^[^/.:\\\\]+$");

    static {
        localConf = readStormConfig();
        serializationDelegate = getSerializationDelegate(localConf);
    }

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static Utils setInstance(Utils u) {
        Utils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    @VisibleForTesting
    public static void setClassLoaderForJavaDeSerialize(ClassLoader cl) {
        Utils.cl = cl;
    }

    @VisibleForTesting
    public static void resetClassLoaderForJavaDeSerialize() {
        Utils.cl = ClassLoader.getSystemClassLoader();
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> findAndReadConfigFile(String name, boolean mustExist) {
        InputStream in = null;
        boolean confFileEmpty = false;
        try {
            in = getConfigFileInputStream(name);
            if (null != in) {
                Yaml yaml = new Yaml(new SafeConstructor());
                @SuppressWarnings("unchecked")
                Map<String, Object> ret = (Map<String, Object>) yaml.load(new InputStreamReader(in));
                if (null != ret) {
                    return new HashMap<>(ret);
                } else {
                    confFileEmpty = true;
                }
            }

            if (mustExist) {
                if (confFileEmpty) {
                    throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
                } else {
                    throw new RuntimeException("Could not find config file on classpath " + name);
                }
            } else {
                return new HashMap<>();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static Map<String, Object> findAndReadConfigFile(String name) {
        return findAndReadConfigFile(name, true);
    }

    private static InputStream getConfigFileInputStream(String configFilePath)
        throws IOException {
        if (null == configFilePath) {
            throw new IOException(
                "Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException(
                "Found multiple " + configFilePath
                + " resources. You're probably bundling the Storm jars with your topology jar. "
                + resources);
        } else {
            LOG.debug("Using " + configFilePath + " from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    public static Map<String, Object> readDefaultConfig() {
        return findAndReadConfigFile("defaults.yaml", true);
    }

    /**
     * URL encode the given string using the UTF-8 charset. Once Storm is baselined to Java 11, we can use URLEncoder.encode(String,
     * Charset) instead, which obsoletes this method.
     */
    public static String urlEncodeUtf8(String s) {
        try {
            return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //This cannot happen since we're using a standard charset
            throw Utils.wrapInRuntime(e);
        }
    }
    
    /**
     * URL decode the given string using the UTF-8 charset. Once Storm is baselined to Java 11, we can use URLDecoder.decode(String,
     * Charset) instead, which obsoletes this method.
     */
    public static String urlDecodeUtf8(String s) {
        try {
            //Once Storm is baselined to Java 11, we can use URLDecoder.decode(String, Charset) instead, which obsoletes this method.
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //This cannot happen since we're using a standard charset
            throw Utils.wrapInRuntime(e);
        }
    }
    
    public static Map<String, Object> readCommandLineOpts() {
        Map<String, Object> ret = new HashMap<>();
        String commandOptions = System.getProperty("storm.options");
        if (commandOptions != null) {
            /*
             Below regex uses negative lookahead to not split in the middle of json objects '{}'
             or json arrays '[]'. This is needed to parse valid json object/arrays passed as options
             via 'storm.cmd' in windows. This is not an issue while using 'storm.py' since it url-encodes
             the options and the below regex just does a split on the commas that separates each option.

             Note:- This regex handles only valid json strings and could produce invalid results
             if the options contain un-encoded invalid json or strings with unmatched '[, ], { or }'. We can
             replace below code with split(",") once 'storm.cmd' is fixed to send url-encoded options.
              */
            String[] configs = commandOptions.split(",(?![^\\[\\]{}]*(]|}))");
            for (String config : configs) {
                config = urlDecodeUtf8(config);
                String[] options = config.split("=", 2);
                if (options.length == 2) {
                    Object val = options[1];
                    try {
                        val = JSONValue.parseWithException(options[1]);
                    } catch (ParseException ignored) {
                        //fall back to string, which is already set
                    }
                    ret.put(options[0], val);
                }
            }
        }
        return ret;
    }

    public static Map<String, Object> readStormConfig() {
        Map<String, Object> ret = readDefaultConfig();
        String confFile = System.getProperty("storm.conf.file");
        Map<String, Object> storm;
        if (confFile == null || confFile.equals("")) {
            storm = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm = findAndReadConfigFile(confFile, true);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());
        return ret;
    }

    public static long bitXorVals(List<Long> coll) {
        long result = 0;
        for (Long val : coll) {
            result ^= val;
        }
        return result;
    }

    public static long bitXor(Long a, Long b) {
        return a ^ b;
    }

    /**
     * Adds the user supplied function as a shutdown hook for cleanup. Also adds a function that sleeps for a second and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithForceKillIn1Sec(Runnable func) {
        addShutdownHookWithDelayedForceKill(func, 1);
    }

    /**
     * Adds the user supplied function as a shutdown hook for cleanup. Also adds a function that sleeps for numSecs and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithDelayedForceKill(Runnable func, int numSecs) {
        final Thread sleepKill = new Thread(() -> {
            try {
                LOG.info("Halting after {} seconds", numSecs);
                Time.sleepSecs(numSecs);
                LOG.warn("Forcing Halt... {}", Utils.threadDump());
                Runtime.getRuntime().halt(20);
            } catch (InterruptedException ie) {
                //Ignored/expected...
            } catch (Exception e) {
                LOG.warn("Exception in the ShutDownHook", e);
            }
        }, "ShutdownHook-sleepKill-" + numSecs + "s");
        sleepKill.setDaemon(true);
        Thread shutdownFunc = new Thread(() -> {
            func.run();
            sleepKill.interrupt();
        }, "ShutdownHook-shutdownFunc");
        Runtime.getRuntime().addShutdownHook(shutdownFunc);
        Runtime.getRuntime().addShutdownHook(sleepKill);
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    /**
     * Creates a thread that calls the given code repeatedly, sleeping for an interval of seconds equal to the return value of the previous
     * call.
     *
     * <p>The given afn may be a callable that returns the number of seconds to sleep, or it may be a Callable that returns another Callable
     * that in turn returns the number of seconds to sleep. In the latter case isFactory.
     *
     * @param afn              the code to call on each iteration
     * @param isDaemon         whether the new thread should be a daemon thread
     * @param eh               code to call when afn throws an exception
     * @param priority         the new thread's priority
     * @param isFactory        whether afn returns a callable instead of sleep seconds
     * @param startImmediately whether to start the thread before returning
     * @param threadName       a suffix to be appended to the thread name
     * @return the newly created thread
     *
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn, boolean isDaemon, final Thread.UncaughtExceptionHandler eh,
                                        int priority, final boolean isFactory, boolean startImmediately,
                                        String threadName) {
        SmartThread thread = new SmartThread(new Runnable() {
            public void run() {
                try {
                    final Callable<Long> fn = isFactory ? (Callable<Long>) afn.call() : afn;
                    while (true) {
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                        final Long s = fn.call();
                        if (s == null) { // then stop running it
                            break;
                        }
                        if (s > 0) {
                            Time.sleep(s);
                        }
                    }
                } catch (Throwable t) {
                    if (Utils.exceptionCauseIsInstanceOf(
                        InterruptedException.class, t)) {
                        LOG.info("Async loop interrupted!");
                        return;
                    }
                    LOG.error("Async loop died!", t);
                    throw new RuntimeException(t);
                }
            }
        });
        if (eh != null) {
            thread.setUncaughtExceptionHandler(eh);
        } else {
            thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Async loop died!", e);
                    Utils.exitProcess(1, "Async loop died!");
                }
            });
        }
        thread.setDaemon(isDaemon);
        thread.setPriority(priority);
        if (threadName != null && !threadName.isEmpty()) {
            thread.setName(thread.getName() + "-" + threadName);
        }
        if (startImmediately) {
            thread.start();
        }
        return thread;
    }

    /**
     * Convenience method used when only the function and name suffix are given.
     *
     * @param afn        the code to call on each iteration
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     *
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn, String threadName, final Thread.UncaughtExceptionHandler eh) {
        return asyncLoop(afn, false, eh, Thread.NORM_PRIORITY, false, true,
                         threadName);
    }

    /**
     * Convenience method used when only the function is given.
     *
     * @param afn the code to call on each iteration
     * @return the newly created thread
     */
    public static SmartThread asyncLoop(final Callable afn) {
        return asyncLoop(afn, false, null, Thread.NORM_PRIORITY, false, true,
                         null);
    }

    /**
     * Checks if a throwable is an instance of a particular class.
     *
     * @param klass     The class you're expecting
     * @param throwable The throwable you expect to be an instance of klass
     * @return true if throwable is instance of klass, false otherwise.
     */
    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        return unwrapTo(klass, throwable) != null;
    }

    public static <T extends Throwable> T unwrapTo(Class<T> klass, Throwable t) {
        while (t != null) {
            if (klass.isInstance(t)) {
                return (T) t;
            }
            t = t.getCause();
        }
        return null;
    }

    public static <T extends Throwable> void unwrapAndThrow(Class<T> klass, Throwable t) throws T {
        T ret = unwrapTo(klass, t);
        if (ret != null) {
            throw ret;
        }
    }

    public static RuntimeException wrapInRuntime(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new RuntimeException(e);
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    /**
     * Gets the storm.local.hostname value, or tries to figure out the local hostname if it is not set in the config.
     *
     * @return a string representation of the hostname.
     */
    public static String hostname() throws UnknownHostException {
        return _instance.hostnameImpl();
    }

    public static String localHostname() throws UnknownHostException {
        return _instance.localHostnameImpl();
    }

    public static void exitProcess(int val, String msg) {
        String combinedErrorMessage = "Halting process: " + msg;
        LOG.error(combinedErrorMessage, new RuntimeException(combinedErrorMessage));
        Runtime.getRuntime().exit(val);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static byte[] javaSerialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T javaDeserialize(byte[] serialized, Class<T> clazz) {
        if ("true".equalsIgnoreCase(System.getProperty("java.deserialization.disabled"))) {
            throw new AssertionError("java deserialization has been disabled and is only safe from within a worker process");
        }

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = null;
            if (null == Utils.cl) {
                ois = new ObjectInputStream(bis);
            } else {
                // Use custom class loader set in testing environment
                ois = new ClassLoaderObjectInputStream(Utils.cl, bis);
            }
            Object ret = ois.readObject();
            ois.close();
            return (T) ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static double zeroIfNaNOrInf(double x) {
        return (Double.isNaN(x) || Double.isInfinite(x)) ? 0.0 : x;
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        StringBuilder ret = new StringBuilder();
        while (it.hasNext()) {
            ret.append(it.next());
            if (it.hasNext()) {
                ret.append(sep);
            }
        }
        return ret.toString();
    }

    public static Id parseZkId(String id, String configName) {
        String[] split = id.split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(configName + " does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        return new Id(split[0], split[1]);
    }

    /**
     * Get the ACL for nimbus/supervisor.  The Super User ACL. This assumes that security is enabled.
     *
     * @param conf the config to get the super User ACL from
     * @return the super user ACL.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static ACL getSuperUserAcl(Map<String, Object> conf) {
        String stormZKUser = (String) conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
            throw new IllegalArgumentException("Authentication is enabled but " + Config.STORM_ZOOKEEPER_SUPERACL + " is not set");
        }
        return new ACL(ZooDefs.Perms.ALL, parseZkId(stormZKUser, Config.STORM_ZOOKEEPER_SUPERACL));
    }

    /**
     * Get the ZK ACLs that a worker should use when writing to ZK.
     *
     * @param conf the config for the topology.
     * @return the ACLs
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static List<ACL> getWorkerACL(Map<String, Object> conf) {
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        ArrayList<ACL> ret = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(getSuperUserAcl(conf));
        return ret;
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     *
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map<String, Object> conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && !((String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Handles uncaught exceptions.
     *
     * @param worker true if this is for handling worker exceptions
     */
    public static void handleUncaughtException(Throwable t, Set<Class<?>> allowedExceptions, boolean worker) {
        if (t != null) {
            if (t instanceof OutOfMemoryError) {
                try {
                    System.err.println("Halting due to Out Of Memory Error..." + Thread.currentThread().getName());
                } catch (Throwable err) {
                    //Again we don't want to exit because of logging issues.
                }
                Runtime.getRuntime().halt(-1);
            }
        }

        if (allowedExceptions.contains(t.getClass())) {
            LOG.info("Swallowing {} {}", t.getClass(), t);
            return;
        }

        if (worker && isAllowedWorkerException(t)) {
            LOG.info("Swallowing {} {}", t.getClass(), t);
            return;
        }

        //Running in daemon mode, we would pass Error to calling thread.
        throw new Error(t);
    }

    public static void handleUncaughtException(Throwable t) {
        handleUncaughtException(t, defaultAllowedExceptions, false);
    }

    public static void handleWorkerUncaughtException(Throwable t) {
        handleUncaughtException(t, defaultAllowedExceptions, true);
    }

    // Hadoop UserGroupInformation can launch an autorenewal thread that can cause a NullPointerException
    // for workers.  See STORM-3606 for an explanation.
    private static boolean isAllowedWorkerException(Throwable t) {
        if (t instanceof NullPointerException) {
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement trace : stackTrace) {
                if (trace.getClassName().startsWith("org.apache.hadoop.security.UserGroupInformation")
                        && trace.getMethodName().equals("run")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static byte[] thriftSerialize(TBase t) {
        try {
            TSerializer ser = threadSer.get();
            if (ser == null) {
                ser = new TSerializer();
                threadSer.set(ser);
            }
            return ser.serialize(t);
        } catch (TException e) {
            LOG.error("Failed to serialize to thrift: ", e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class<T> c, byte[] b) {
        try {
            return thriftDeserialize(c, b, 0, b.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class<T> c, byte[] b, int offset, int length) {
        try {
            T ret = c.newInstance();
            TDeserializer des = getDes();
            des.deserialize((TBase) ret, b, offset, length);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static TDeserializer getDes() {
        TDeserializer des = threadDes.get();
        if (des == null) {
            des = new TDeserializer();
            threadDes.set(des);
        }
        return des;
    }

    public static void sleepNoSimulation(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
    
    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static UptimeComputer makeUptimeComputer() {
        return _instance.makeUptimeComputerImpl();
    }

    /**
     * <code>"{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"</code>.
     *
     * <p>Example usage in java:
     * <code>Map&lt;Integer, String&gt; tasks; Map&lt;String, List&lt;Integer&gt;&gt; componentTasks = Utils.reverse_map(tasks);</code>
     *
     * <p>The order of he resulting list values depends on the ordering properties of the Map passed in. The caller is
     * responsible for passing an ordered map if they expect the result to be consistently ordered as well.
     *
     * @param map to reverse
     * @return a reversed map
     */
    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }

    /**
     * "[[:a 1] [:b 1] [:c 2]} -> {1 [:a :b] 2 :c}" Reverses an assoc-list style Map like reverseMap(Map...)
     *
     * @param listSeq to reverse
     * @return a reversed map
     */
    public static Map<Object, List<Object>> reverseMap(List<List<Object>> listSeq) {
        Map<Object, List<Object>> rtn = new HashMap<>();
        if (listSeq == null) {
            return rtn;
        }
        for (List<Object> listEntry : listSeq) {
            Object key = listEntry.get(0);
            Object val = listEntry.get(1);
            List<Object> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<>();
                rtn.put(val, list);
            }
            list.add(key);
        }
        return rtn;
    }

    public static boolean isOnWindows() {
        if (System.getenv("OS") != null) {
            return System.getenv("OS").equals("Windows_NT");
        }
        return false;
    }

    public static boolean checkFileExists(String path) {
        return Files.exists(new File(path).toPath());
    }

    /**
     * Deletes a file or directory and its contents if it exists. Does not complain if the input is null or does not exist.
     *
     * @param path the path to the file or directory
     */
    public static void forceDelete(String path) throws IOException {
        _instance.forceDeleteImpl(path);
    }

    public static byte[] serialize(Object obj) {
        return serializationDelegate.serialize(obj);
    }

    public static <T> T deserialize(byte[] serialized, Class<T> clazz) {
        return serializationDelegate.deserialize(serialized, clazz);
    }

    /**
     * Serialize an object using the configured serialization and then base64 encode it into a string.
     *
     * @param obj the object to encode
     * @return a string with the encoded object in it.
     */
    public static String serializeToString(Object obj) {
        return Base64.getEncoder().encodeToString(serializationDelegate.serialize(obj));
    }

    /**
     * Deserialize an object stored in a string. The String is assumed to be a base64 encoded string containing the bytes to actually
     * deserialize.
     *
     * @param str   the encoded string.
     * @param clazz the thrift class we are expecting.
     * @param <T>   The type of clazz
     * @return the decoded object
     */
    public static <T> T deserializeFromString(String str, Class<T> clazz) {
        return deserialize(Base64.getDecoder().decode(str), clazz);
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static Runnable mkSuicideFn() {
        return new Runnable() {
            @Override
            public void run() {
                exitProcess(1, "Worker died");
            }
        };
    }

    public static void readAndLogStream(String prefix, InputStream in) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = r.readLine()) != null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error while trying to log stream", e);
        }
    }

    /**
     * Creates an instance of the pluggable SerializationDelegate or falls back to DefaultSerializationDelegate if something goes wrong.
     *
     * @param topoConf The config from which to pull the name of the pluggable class.
     * @return an instance of the class specified by storm.meta.serialization.delegate
     */
    private static SerializationDelegate getSerializationDelegate(Map<String, Object> topoConf) {
        String delegateClassName = (String) topoConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to construct serialization delegate class " + delegateClassName, e);
        }
        delegate.prepare(topoConf);
        return delegate;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String id) {
        if (topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if (topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if (topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(bos);
            out.write(data);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] gunzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream in = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = in.read(buffer)) >= 0) {
                bos.write(buffer, 0, len);
            }
            in.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getRepeat(List<String> list) {
        List<String> rtn = new ArrayList<String>();
        Set<String> idSet = new HashSet<String>();

        for (String id : list) {
            if (idSet.contains(id)) {
                rtn.add(id);
            } else {
                idSet.add(id);
            }
        }

        return rtn;
    }

    public static GlobalStreamId getGlobalStreamId(String componentId, String streamId) {
        if (streamId == null) {
            return new GlobalStreamId(componentId, DEFAULT_STREAM_ID);
        }
        return new GlobalStreamId(componentId, streamId);
    }

    public static Object getSetComponentObject(ComponentObject obj) {
        if (obj.getSetField() == ComponentObject._Fields.SERIALIZED_JAVA) {
            return javaDeserialize(obj.get_serialized_java(), Serializable.class);
        } else if (obj.getSetField() == ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is positive, the original value is returned.
     * When the input number is negative, the returned positive value is the original value bit AND against Integer.MAX_VALUE(0x7fffffff)
     * which is not its absolutely value.
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & Integer.MAX_VALUE;
    }

    /**
     * Get process PID.
     * @return the pid of this JVM, because Java doesn't provide a real way to do this.
     */
    public static String processPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }
        return split[0];
    }

    public static Map<String, Object> fromCompressedJsonConf(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            InputStreamReader in = new InputStreamReader(new GZIPInputStream(bis));
            Object ret = JSONValue.parseWithException(in);
            in.close();
            return (Map<String, Object>) ret;
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new map with a string value in the map replaced with an equivalently-lengthed string of '#'.  (If the object is not a
     * string to string will be called on it and replaced)
     *
     * @param m   The map that a value will be redacted from
     * @param key The key pointing to the value to be redacted
     * @return a new map with the value redacted. The original map will not be modified.
     */
    public static Map<String, Object> redactValue(Map<String, Object> m, String key) {
        if (m.containsKey(key)) {
            HashMap<String, Object> newMap = new HashMap<>(m);
            Object value = newMap.get(key);
            String v = value.toString();
            String redacted = new String(new char[v.length()]).replace("\0", "#");
            newMap.put(key, redacted);
            return newMap;
        }
        return m;
    }

    public static UncaughtExceptionHandler createDefaultUncaughtExceptionHandler() {
        return (thread, thrown) -> {
            try {
                handleUncaughtException(thrown);
            } catch (Error err) {
                LOG.error("Received error in thread {}.. terminating server...", thread.getName(), err);
                Runtime.getRuntime().exit(-2);
            }
        };
    }

    public static UncaughtExceptionHandler createWorkerUncaughtExceptionHandler() {
        return (thread, thrown) -> {
            try {
                handleWorkerUncaughtException(thrown);
            } catch (Error err) {
                LOG.error("Received error in thread {}.. terminating worker...", thread.getName(), err);
                Runtime.getRuntime().exit(-2);
            }
        };
    }

    public static void setupDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(createDefaultUncaughtExceptionHandler());
    }

    public static void setupWorkerUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(createWorkerUncaughtExceptionHandler());
    }

    /**
     * parses the arguments to extract jvm heap memory size in MB.
     *
     * @return the value of the JVM heap memory setting (in MB) in a java command.
     */
    public static Double parseJvmHeapMemByChildOpts(List<String> options, Double defaultValue) {
        if (options != null) {
            Pattern optsPattern = Pattern.compile("Xmx([0-9]+)([mkgMKG])");
            for (String option : options) {
                if (option == null) {
                    continue;
                }
                Matcher m = optsPattern.matcher(option);
                while (m.find()) {
                    long value = Long.parseLong(m.group(1));
                    char unitChar = m.group(2).toLowerCase().charAt(0);
                    int unit;
                    switch (unitChar) {
                        case 'k':
                            unit = 1024;
                            break;
                        case 'm':
                            unit = 1024 * 1024;
                            break;
                        case 'g':
                            unit = 1024 * 1024 * 1024;
                            break;
                        default:
                            unit = 1;
                    }
                    Double result = value * unit / 1024.0 / 1024.0;
                    return (result < 1.0) ? 1.0 : result;
                }
            }
            return defaultValue;
        } else {
            return defaultValue;
        }
    }

    public static ClientBlobStore getClientBlobStore(Map<String, Object> conf) {
        ClientBlobStore store = (ClientBlobStore) ReflectionUtils.newInstance((String) conf.get(Config.CLIENT_BLOBSTORE));
        store.prepare(conf);
        return store;
    }

    @SuppressWarnings("unchecked")
    private static Object normalizeConfValue(Object obj) {
        if (obj instanceof Map) {
            return normalizeConf((Map<String, Object>) obj);
        } else if (obj instanceof Collection) {
            List<Object> confList = new ArrayList<>((Collection<Object>) obj);
            for (int i = 0; i < confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConfValue(val));
            }
            return confList;
        } else if (obj instanceof Integer) {
            return ((Number) obj).longValue();
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else {
            return obj;
        }
    }

    private static Map<String, Object> normalizeConf(Map<String, Object> conf) {
        if (conf == null) {
            return new HashMap<>();
        }
        Map<String, Object> ret = new HashMap<>(conf);
        for (Map.Entry<String, Object> entry : ret.entrySet()) {
            ret.put(entry.getKey(), normalizeConfValue(entry.getValue()));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static boolean isValidConf(Map<String, Object> topoConfIn) {
        Map<String, Object> origTopoConf = normalizeConf(topoConfIn);
        try {
            Map<String, Object> deserTopoConf = normalizeConf(
                (Map<String, Object>) JSONValue.parseWithException(JSONValue.toJSONString(topoConfIn)));
            return isValidConf(origTopoConf, deserTopoConf);
        } catch (ParseException e) {
            LOG.error("Json serialized config could not be deserialized", e);
        }
        return false;
    }

    @VisibleForTesting
    static boolean isValidConf(Map<String, Object> orig, Map<String, Object> deser) {
        MapDifference<String, Object> diff = Maps.difference(orig, deser);
        if (diff.areEqual()) {
            return true;
        }
        for (Map.Entry<String, Object> entryOnLeft : diff.entriesOnlyOnLeft().entrySet()) {
            LOG.warn("Config property ({}) is found in original config, but missing from the "
                     + "serialized-deserialized config. This is due to an internal error in "
                     + "serialization. Name: {} - Value: {}",
                     entryOnLeft.getKey(), entryOnLeft.getKey(), entryOnLeft.getValue());
        }
        for (Map.Entry<String, Object> entryOnRight : diff.entriesOnlyOnRight().entrySet()) {
            LOG.warn("Config property ({}) is not found in original config, but present in "
                     + "serialized-deserialized config. This is due to an internal error in "
                     + "serialization. Name: {} - Value: {}",
                     entryOnRight.getKey(), entryOnRight.getKey(), entryOnRight.getValue());
        }
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entryDiffers : diff.entriesDiffering().entrySet()) {
            Object leftValue = entryDiffers.getValue().leftValue();
            Object rightValue = entryDiffers.getValue().rightValue();
            LOG.warn("Config value differs after json serialization. Name: {} - Original Value: {} - DeSer. Value: {}",
                     entryDiffers.getKey(), leftValue, rightValue);
        }
        return false;
    }

    public static TopologyInfo getTopologyInfo(String name, String asUser, Map<String, Object> topoConf) {
        try (NimbusClient client = NimbusClient.getConfiguredClientAs(topoConf, asUser)) {
            return client.getClient().getTopologyInfoByName(name);
        } catch (NotAliveException notAliveException) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTopologyId(String name, Nimbus.Iface client) {
        try {
            TopologySummary topologySummary = client.getTopologySummaryByName(name);
            if (topologySummary != null) {
                return topologySummary.get_id();
            }
        } catch (NotAliveException notAliveException) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Validate topology blobstore map.
     *
     * @param topoConf Topology configuration
     */
    public static void validateTopologyBlobStoreMap(Map<String, Object> topoConf) throws InvalidTopologyException, AuthorizationException {
        try (NimbusBlobStore client = new NimbusBlobStore()) {
            client.prepare(topoConf);
            validateTopologyBlobStoreMap(topoConf, client);
        }
    }

    /**
     * Validate topology blobstore map.
     *
     * @param topoConf Topology configuration
     * @param client   The NimbusBlobStore client. It must call prepare() before being used here.
     */
    public static void validateTopologyBlobStoreMap(Map<String, Object> topoConf, NimbusBlobStore client)
        throws InvalidTopologyException, AuthorizationException {
        Map<String, Map<String, Object>> blobStoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        if (blobStoreMap != null) {
            for (String key : blobStoreMap.keySet()) {

                Map<String, Object> blobConf = blobStoreMap.get(key);
                try {
                    ObjectReader.getBoolean(blobConf.get("uncompress"), false);
                    ObjectReader.getBoolean(blobConf.get("workerRestart"), false);
                } catch (IllegalArgumentException e) {
                    throw new WrappedInvalidTopologyException("Invalid blob conf option: " + e.getMessage());
                }

                // try to get BlobMeta
                // This will check if the key exists and if the subject has authorization
                try {
                    client.getBlobMeta(key);
                } catch (KeyNotFoundException keyNotFound) {
                    // wrap KeyNotFoundException in an InvalidTopologyException
                    throw new WrappedInvalidTopologyException("Key not found: " + keyNotFound.get_msg());
                }
            }
        }
    }

    /**
     * Validate topology blobstore map.
     */
    public static void validateTopologyBlobStoreMap(Map<String, Object> topoConf, BlobStore blobStore)
        throws InvalidTopologyException, AuthorizationException {
        Map<String, Object> blobStoreMap = (Map<String, Object>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        if (blobStoreMap != null) {
            Subject subject = ReqContext.context().subject();
            for (String key : blobStoreMap.keySet()) {
                try {
                    blobStore.getBlobMeta(key, subject);
                } catch (KeyNotFoundException keyNotFound) {
                    // wrap KeyNotFoundException in an InvalidTopologyException
                    throw new WrappedInvalidTopologyException("Key not found: " + keyNotFound.get_msg());
                }
            }
        }
    }

    /**
     * Gets some information, including stack trace, for a running thread.
     *
     * @return A human-readable string of the dump.
     */
    public static String threadDump() {
        final StringBuilder dump = new StringBuilder();
        final java.lang.management.ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        final java.lang.management.ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(threadMxBean.getAllThreadIds(), 100);
        for (Entry<Thread, StackTraceElement[]> entry: Thread.getAllStackTraces().entrySet()) {
            Thread t = entry.getKey();
            ThreadInfo threadInfo = threadMxBean.getThreadInfo(t.getId());
            if (threadInfo == null) {
                //Thread died before we could get the info, skip
                continue;
            }
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            if (t.isDaemon()) {
                dump.append("(DAEMON)");
            }
            dump.append("\n   lock: ");
            dump.append(threadInfo.getLockName());
            dump.append(" owner: ");
            dump.append(threadInfo.getLockOwnerName());
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            for (final StackTraceElement stackTraceElement : entry.getValue()) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    public static boolean checkDirExists(String dir) {
        File file = new File(dir);
        return file.isDirectory();
    }

    /**
     * Return a new instance of a pluggable specified in the conf.
     *
     * @param conf      The conf to read from.
     * @param configKey The key pointing to the pluggable class
     * @return an instance of the class or null if it is not specified.
     */
    public static Object getConfiguredClass(Map<String, Object> conf, Object configKey) {
        if (conf.containsKey(configKey)) {
            return ReflectionUtils.newInstance((String) conf.get(configKey));
        }
        return null;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way? This only works when called from within Nimbus or a Supervisor
     * process.
     *
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map<String, Object> conf) {
        return null != System.getProperty("java.security.auth.login.config")
               || (conf != null
                   && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                   && !((String) conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    public static byte[] toCompressedJsonConf(Map<String, Object> topoConf) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamWriter out = new OutputStreamWriter(new GZIPOutputStream(bos));
            JSONValue.writeJSONString(topoConf, out);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static double nullToZero(Double v) {
        return (v != null ? v : 0);
    }

    /**
     * a or b the first one that is not null.
     *
     * @param a something
     * @param b something else
     * @return a or b the first one that is not null
     */
    @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MethodName"})
    public static <V> V OR(V a, V b) {
        return a == null ? b : a;
    }

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base + 1, numInc);
        }
        return ret;
    }

    /**
     * Fills up chunks out of a collection (given a maximum amount of chunks).
     *
     * <p>i.e. partitionFixed(5, [1,2,3]) -> [[1,2,3]] partitionFixed(5, [1..9]) -> [[1,2], [3,4], [5,6], [7,8], [9]] partitionFixed(3,
     * [1..10]) -> [[1,2,3,4], [5,6,7], [8,9,10]]
     *
     * @param maxNumChunks the maximum number of chunks to return
     * @param coll         the collection to be chunked up
     * @return a list of the chunks, which are themselves lists.
     */
    public static <T> List<List<T>> partitionFixed(int maxNumChunks, Collection<T> coll) {
        List<List<T>> ret = new ArrayList<>();

        if (maxNumChunks == 0 || coll == null) {
            return ret;
        }

        Map<Integer, Integer> parts = integerDivided(coll.size(), maxNumChunks);

        // Keys sorted in descending order
        List<Integer> sortedKeys = new ArrayList<Integer>(parts.keySet());
        Collections.sort(sortedKeys, Collections.reverseOrder());


        Iterator<T> it = coll.iterator();
        for (Integer chunkSize : sortedKeys) {
            if (!it.hasNext()) {
                break;
            }
            Integer times = parts.get(chunkSize);
            for (int i = 0; i < times; i++) {
                if (!it.hasNext()) {
                    break;
                }
                List<T> chunkList = new ArrayList<>();
                for (int j = 0; j < chunkSize; j++) {
                    if (!it.hasNext()) {
                        break;
                    }
                    chunkList.add(it.next());
                }
                ret.add(chunkList);
            }
        }

        return ret;
    }

    public static Object readYamlFile(String yamlFile) {
        try (FileReader reader = new FileReader(yamlFile)) {
            return new Yaml(new SafeConstructor()).load(reader);
        } catch (Exception ex) {
            LOG.error("Failed to read yaml file.", ex);
        }
        return null;
    }

    /**
     * Gets an available port. Consider if it is possible to pass port 0 to the server instead of using this method, since there is no
     * guarantee that the port returned by this method will remain free.
     *
     * @return The preferred port if available, or a random available port
     */
    public static int getAvailablePort(int preferredPort) {
        int localPort = -1;
        try (ServerSocket socket = new ServerSocket(preferredPort)) {
            localPort = socket.getLocalPort();
        } catch (IOException exp) {
            if (preferredPort > 0) {
                return getAvailablePort(0);
            }
        }
        return localPort;
    }

    /**
     * Shortcut to calling {@link #getAvailablePort(int) } with 0 as the preferred port.
     *
     * @return A random available port
     */
    public static int getAvailablePort() {
        return getAvailablePort(0);
    }

    /**
     * Find the first item of coll for which pred.test(...) returns true.
     *
     * @param pred The IPredicate to test for
     * @param coll The Collection of items to search through.
     * @return The first matching value in coll, or null if nothing matches.
     */
    public static <T> T findOne(IPredicate<T> pred, Collection<T> coll) {
        if (coll == null) {
            return null;
        }
        for (T elem : coll) {
            if (pred.test(elem)) {
                return elem;
            }
        }
        return null;
    }

    public static <T, U> T findOne(IPredicate<T> pred, Map<U, T> map) {
        if (map == null) {
            return null;
        }
        return findOne(pred, (Set<T>) map.entrySet());
    }

    public static Map<String, Object> parseJson(String json) {
        if (json == null) {
            return new HashMap<>();
        } else {
            try {
                return (Map<String, Object>) JSONValue.parseWithException(json);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String memoizedLocalHostname() throws UnknownHostException {
        if (memoizedLocalHostnameString == null) {
            memoizedLocalHostnameString = localHostname();
        }
        return memoizedLocalHostnameString;
    }

    /**
     * Add version information to the given topology.
     *
     * @param topology the topology being submitted (MIGHT BE MODIFIED)
     * @return topology
     */
    public static StormTopology addVersions(StormTopology topology) {
        String stormVersion = VersionInfo.getVersion();
        if (stormVersion != null
            && !"Unknown".equalsIgnoreCase(stormVersion)
            && !topology.is_set_storm_version()) {
            topology.set_storm_version(stormVersion);
        }

        String jdkVersion = System.getProperty("java.version");
        if (jdkVersion != null && !topology.is_set_jdk_version()) {
            topology.set_jdk_version(jdkVersion);
        }
        return topology;
    }

    /**
     * Get a map of version to classpath from the conf Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP
     *
     * @param conf      what to read it out of
     * @param currentClassPath the current classpath for this version of storm (not included in the conf, but returned by this)
     * @return the map
     */
    public static NavigableMap<SimpleVersion, List<String>> getConfiguredClasspathVersions(Map<String, Object> conf,
                                                                                           List<String> currentClassPath) {
        TreeMap<SimpleVersion, List<String>> ret = new TreeMap<>();
        Map<String, String> fromConf =
            (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry : fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), Arrays.asList(entry.getValue().split(File.pathSeparator)));
        }
        ret.put(VersionInfo.OUR_VERSION, currentClassPath);
        return ret;
    }

    /**
     * Get a mapping of the configured supported versions of storm to their actual versions.
     * @param conf what to read the configuration out of.
     * @return the map.
     */
    public static NavigableMap<String, IVersionInfo> getAlternativeVersionsMap(Map<String, Object> conf) {
        TreeMap<String, IVersionInfo> ret = new TreeMap<>();
        Map<String, String> fromConf =
            (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry : fromConf.entrySet()) {
            IVersionInfo version = VersionInfo.getFromClasspath(entry.getValue());
            if (version != null) {
                ret.put(entry.getKey(), version);
            } else {
                LOG.error("Could not find the real version of {} from CP {}", entry.getKey(), entry.getValue());
                ret.put(entry.getKey(), new IVersionInfo() {
                    @Override
                    public String getVersion() {
                        return "Unknown";
                    }

                    @Override
                    public String getRevision() {
                        return "Unknown";
                    }

                    @Override
                    public String getBranch() {
                        return "Unknown";
                    }

                    @Override
                    public String getDate() {
                        return "Unknown";
                    }

                    @Override
                    public String getUser() {
                        return "Unknown";
                    }

                    @Override
                    public String getUrl() {
                        return "Unknown";
                    }

                    @Override
                    public String getSrcChecksum() {
                        return "Unknown";
                    }

                    @Override
                    public String getBuildVersion() {
                        return "Unknown";
                    }
                });
            }
        }
        return ret;
    }

    /**
     * Get a map of version to worker main from the conf Config.SUPERVISOR_WORKER_VERSION_MAIN_MAP
     *
     * @param conf what to read it out of
     * @return the map
     */
    public static NavigableMap<SimpleVersion, String> getConfiguredWorkerMainVersions(Map<String, Object> conf) {
        TreeMap<SimpleVersion, String> ret = new TreeMap<>();
        Map<String, String> fromConf =
            (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_MAIN_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry : fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), entry.getValue());
        }

        ret.put(VersionInfo.OUR_VERSION, "org.apache.storm.daemon.worker.Worker");
        return ret;
    }

    /**
     * Get a map of version to worker log writer from the conf Config.SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP
     *
     * @param conf what to read it out of
     * @return the map
     */
    public static NavigableMap<SimpleVersion, String> getConfiguredWorkerLogWriterVersions(Map<String, Object> conf) {
        TreeMap<SimpleVersion, String> ret = new TreeMap<>();
        Map<String, String> fromConf =
            (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry : fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), entry.getValue());
        }

        ret.put(VersionInfo.OUR_VERSION, "org.apache.storm.LogWriter");
        return ret;
    }

    public static <T> T getCompatibleVersion(NavigableMap<SimpleVersion, T> versionedMap, SimpleVersion desiredVersion, String what,
                                             T defaultValue) {
        Entry<SimpleVersion, T> ret = versionedMap.ceilingEntry(desiredVersion);
        if (ret == null || ret.getKey().getMajor() != desiredVersion.getMajor()) {
            //Could not find a "fully" compatible version.  Look to see if there is a possibly compatible version right below it
            ret = versionedMap.floorEntry(desiredVersion);
            if (ret == null || ret.getKey().getMajor() != desiredVersion.getMajor()) {
                if (defaultValue != null) {
                    LOG.warn("Could not find any compatible {} falling back to using {}", what, defaultValue);
                }
                return defaultValue;
            }
            LOG.warn("Could not find a higer compatible version for {} {}, using {} instead", what, desiredVersion, ret.getKey());
        }
        return ret.getValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readConfIgnoreNotFound(Yaml yaml, File f) throws IOException {
        Map<String, Object> ret = null;
        if (f.exists()) {
            try (FileReader fr = new FileReader(f)) {
                ret = (Map<String, Object>) yaml.load(fr);
            }
        }
        return ret;
    }

    public static Map<String, Object> getConfigFromClasspath(List<String> cp, Map<String, Object> conf) throws IOException {
        if (cp == null || cp.isEmpty()) {
            return conf;
        }
        Yaml yaml = new Yaml(new SafeConstructor());
        Map<String, Object> defaultsConf = null;
        Map<String, Object> stormConf = null;

        // Based on how Java handles the classpath
        // https://docs.oracle.com/javase/8/docs/technotes/tools/unix/classpath.html
        for (String part : cp) {
            File f = new File(part);

            if (f.getName().equals("*")) {
                // wildcard is given in file
                // in java classpath, '*' is expanded to all jar/JAR files in the directory
                File dir = f.getParentFile();
                if (dir == null) {
                    // it happens when part is just '*' rather than denoting some directory
                    dir = new File(".");
                }

                File[] jarFiles = dir.listFiles((dir1, name) -> name.endsWith(".jar") || name.endsWith(".JAR"));

                // Quoting Javadoc in File.listFiles(FilenameFilter filter):
                // Returns {@code null} if this abstract pathname does not denote a directory, or if an I/O error occurs.
                // Both things are not expected and should not happen.
                if (jarFiles == null) {
                    throw new IOException("Fail to list jar files in directory: " + dir);
                }

                for (File jarFile : jarFiles) {
                    JarConfigReader jarConfigReader = new JarConfigReader(yaml, defaultsConf, stormConf, jarFile).readJar();
                    defaultsConf = jarConfigReader.getDefaultsConf();
                    stormConf = jarConfigReader.getStormConf();
                }
            } else if (f.isDirectory()) {
                // no wildcard, directory
                if (defaultsConf == null) {
                    defaultsConf = readConfIgnoreNotFound(yaml, new File(f, "defaults.yaml"));
                }

                if (stormConf == null) {
                    stormConf = readConfIgnoreNotFound(yaml, new File(f, "storm.yaml"));
                }
            } else if (f.isFile()) {
                // no wildcard, file
                String fileName = f.getName();
                if (fileName.endsWith(".zip") || fileName.endsWith(".ZIP")) {
                    JarConfigReader jarConfigReader = new JarConfigReader(yaml, defaultsConf, stormConf, f).readZip();
                    defaultsConf = jarConfigReader.getDefaultsConf();
                    stormConf = jarConfigReader.getStormConf();
                } else if (fileName.endsWith(".jar") || fileName.endsWith(".JAR")) {
                    JarConfigReader jarConfigReader = new JarConfigReader(yaml, defaultsConf, stormConf, f).readJar();
                    defaultsConf = jarConfigReader.getDefaultsConf();
                    stormConf = jarConfigReader.getStormConf();
                }
                // Class path entries that are neither directories nor archives (.zip or JAR files)
                // nor the asterisk (*) wildcard character are ignored.
            }
        }
        if (stormConf != null) {
            defaultsConf.putAll(stormConf);
        }
        return defaultsConf;
    }

    public static boolean isLocalhostAddress(String address) {
        return LOCALHOST_ADDRESSES.contains(address);
    }

    public static <K, V> Map<K, V> merge(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> other) {
        Map<K, V> ret = new HashMap<>(first);
        if (other != null) {
            ret.putAll(other);
        }
        return ret;
    }

    public static <V> ArrayList<V> convertToArray(Map<Integer, V> srcMap, int start) {
        Set<Integer> ids = srcMap.keySet();
        Integer largestId = ids.stream().max(Integer::compareTo).get();
        int end = largestId - start;
        ArrayList<V> result = new ArrayList<>(Collections.nCopies(end + 1, null)); // creates array[largestId+1] filled with nulls
        for (Map.Entry<Integer, V> entry : srcMap.entrySet()) {
            int id = entry.getKey();
            if (id < start) {
                LOG.debug("Entry {} will be skipped it is too small {} ...", id, start);
            } else {
                result.set(id - start, entry.getValue());
            }
        }
        return result;
    }

    // Non-static impl methods exist for mocking purposes.
    protected void forceDeleteImpl(String path) throws IOException {
        LOG.debug("Deleting path {}", path);
        if (checkFileExists(path)) {
            try {
                FileUtils.forceDelete(new File(path));
            } catch (FileNotFoundException ignored) {
                //ignore
            }
        }
    }

    // Non-static impl methods exist for mocking purposes.
    public UptimeComputer makeUptimeComputerImpl() {
        return new UptimeComputer();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String localHostnameImpl() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String hostnameImpl() throws UnknownHostException {
        if (localConf == null) {
            return memoizedLocalHostname();
        }
        Object hostnameString = localConf.get(Config.STORM_LOCAL_HOSTNAME);
        if (hostnameString == null || hostnameString.equals("")) {
            return memoizedLocalHostname();
        }
        return (String) hostnameString;
    }

    /**
     * Validates blob key.
     *
     * @param key Key for the blob.
     */
    public static boolean isValidKey(String key) {
        if (StringUtils.isEmpty(key) || "..".equals(key) || ".".equals(key) || !BLOB_KEY_PATTERN.matcher(key).matches()) {
            LOG.error("'{}' does not appear to be valid. It must match {}. And it can't be \".\", \"..\", null or empty string.", key,
                BLOB_KEY_PATTERN);
            return false;
        }
        return true;
    }

    /**
     * Validates topology name.
     * @param name the topology name
     * @throws IllegalArgumentException if the topology name is not valid
     */
    public static void validateTopologyName(String name) throws IllegalArgumentException {
        if (name == null || !TOPOLOGY_NAME_REGEX.matcher(name).matches()) {
            String message = "Topology name '" + name + "' is not valid. It can't be null and it must match " + TOPOLOGY_NAME_REGEX;
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * A thread that can answer if it is sleeping in the case of simulated time. This class is not useful when simulated time is not being
     * used.
     */
    public static class SmartThread extends Thread {
        public SmartThread(Runnable r) {
            super(r);
        }

        public boolean isSleeping() {
            return Time.isThreadWaiting(this);
        }
    }

    public static class UptimeComputer {
        int startTime = 0;

        public UptimeComputer() {
            startTime = Time.currentTimeSecs();
        }

        public int upTime() {
            return Time.deltaSecs(startTime);
        }
    }

    private static class JarConfigReader {
        private Yaml yaml;
        private Map<String, Object> defaultsConf;
        private Map<String, Object> stormConf;
        private File file;

        JarConfigReader(Yaml yaml, Map<String, Object> defaultsConf, Map<String, Object> stormConf, File file) {
            this.yaml = yaml;
            this.defaultsConf = defaultsConf;
            this.stormConf = stormConf;
            this.file = file;
        }

        public Map<String, Object> getDefaultsConf() {
            return defaultsConf;
        }

        public Map<String, Object> getStormConf() {
            return stormConf;
        }

        public JarConfigReader readZip() throws IOException {
            try (ZipFile zipFile = new ZipFile(file)) {
                readArchive(zipFile);
            }
            return this;
        }

        public JarConfigReader readJar() throws IOException {
            try (JarFile jarFile = new JarFile(file)) {
                readArchive(jarFile);
            }
            return this;
        }

        private void readArchive(ZipFile zipFile) throws IOException {
            Enumeration<? extends ZipEntry> zipEnums = zipFile.entries();
            while (zipEnums.hasMoreElements()) {
                ZipEntry entry = zipEnums.nextElement();
                if (!entry.isDirectory()) {
                    if (defaultsConf == null && entry.getName().equals("defaults.yaml")) {
                        try (InputStreamReader isr = new InputStreamReader(zipFile.getInputStream(entry))) {
                            defaultsConf = (Map<String, Object>) yaml.load(isr);
                        }
                    }

                    if (stormConf == null && entry.getName().equals("storm.yaml")) {
                        try (InputStreamReader isr = new InputStreamReader(zipFile.getInputStream(entry))) {
                            stormConf = (Map<String, Object>) yaml.load(isr);
                        }
                    }
                }
            }
        }
    }

    /**
     * Create a map of forward edges for bolts in a topology. Note that spouts can be source but not a target in
     * the edge. The mapping contains ids of spouts and bolts.
     *
     * @param topology StormTopology to examine.
     * @return a map with entry for each SpoutId/BoltId to a set of outbound edges of BoltIds.
     */
    private static Map<String, Set<String>> getStormTopologyForwardGraph(StormTopology topology) {
        Map<String, Set<String>> edgesOut = new HashMap<>();

        if (topology.get_bolts() != null) {
            topology.get_bolts().entrySet().forEach(entry -> {
                if (!Utils.isSystemId(entry.getKey())) {
                    entry.getValue().get_common().get_inputs().forEach((k, v) -> {
                        edgesOut.computeIfAbsent(k.get_componentId(), x -> new HashSet<>()).add(entry.getKey());
                    });
                }
            });
        }
        return edgesOut;
    }

    /**
     * Use recursive descent to detect cycles. This is a Depth First recursion. Component Cycle is recorded when encountered.
     * In addition, the last link in the cycle is removed to avoid re-detecting same cycle/subcycle.
     *
     * @param stack used for recursion.
     * @param edgesOut outbound edge connections, modified when cycle is detected.
     * @param seen keeps track of component ids that have already been seen.
     * @param cycles list of cycles seen so far.
     */
    private static void findComponentCyclesRecursion(
            Stack<String> stack, Map<String, Set<String>> edgesOut, Set<String> seen, List<List<String>> cycles) {
        if (stack.isEmpty()) {
            return;
        }
        String compId1 = stack.peek();
        if (!edgesOut.containsKey(compId1) || edgesOut.get(compId1).isEmpty()) {
            stack.pop();
            return;
        }
        Set<String> children = new HashSet<>(edgesOut.get(compId1));
        for (String compId2: children) {
            if (seen.contains(compId2)) {
                // cycle/diamond detected
                List<String> possibleCycle = new ArrayList<>();
                if (compId1.equals(compId2)) {
                    possibleCycle.add(compId2);
                } else if (edgesOut.get(compId2) != null && edgesOut.get(compId2).contains(compId1)) {
                    possibleCycle.addAll(Arrays.asList(compId1, compId2));
                } else {
                    List<String> tmp = Collections.list(stack.elements());
                    int prevIdx = tmp.indexOf(compId2);
                    if (prevIdx >= 0) {
                        // cycle (as opposed to diamond)
                        tmp = tmp.subList(prevIdx, tmp.size());
                        tmp.add(compId2);
                        possibleCycle.addAll(tmp);
                    }
                }
                if (!possibleCycle.isEmpty()) {
                    cycles.add(possibleCycle);
                    edgesOut.get(compId1).remove(compId2); // disconnect this cycle
                    continue;
                }
            }
            seen.add(compId2);
            stack.push(compId2);
            findComponentCyclesRecursion(stack, edgesOut, seen, cycles);
        }
        stack.pop();
    }

    /**
     * Find and return components cycles in the topology graph when starting from spout.
     * Return a list of cycles. Each cycle may consist of one or more components.
     * Components that cannot be reached from any of the spouts are ignored.
     *
     * @return a List of cycles. Each cycle has a list of component names.
     *
     */
    @VisibleForTesting
    public static List<List<String>> findComponentCycles(StormTopology topology, String topoId) {
        List<List<String>> ret = new ArrayList<>();
        Map<String, Set<String>> edgesOut = getStormTopologyForwardGraph(topology);
        Set<String> allComponentIds = new HashSet<>();
        edgesOut.forEach((k, v) -> {
            allComponentIds.add(k) ;
            allComponentIds.addAll(v);
        });

        if (topology.get_spouts_size() == 0) {
            LOG.error("Topology {} does not contain any spouts, cannot traverse graph to determine cycles", topoId);
            return ret;
        }

        Set<String> unreachable = new HashSet<>(edgesOut.keySet());
        topology.get_spouts().forEach((spoutId, spout)  -> {
            Stack<String> dfsStack = new Stack<>();
            dfsStack.push(spoutId);
            Set<String> seen = new HashSet<>();
            seen.add(spoutId);
            findComponentCyclesRecursion(dfsStack, edgesOut, seen, ret);
            unreachable.removeAll(seen);
        });

        // warning about unreachable components
        if (!unreachable.isEmpty()) {
            LOG.warn("Topology {} contains unreachable components \"{}\"", topoId, String.join(",", unreachable));
        }
        return ret;
    }

    /**
     * Validate that the topology is cycle free. If not, then throw an InvalidTopologyException describing the cycle(s).
     *
     * @param topology StormTopology instance to examine.
     * @param name Name of the topology, used in exception error message.
     * @throws InvalidTopologyException if there are cycles, with message describing the cycles encountered.
     */
    public static void validateCycleFree(StormTopology topology, String name) throws InvalidTopologyException {
        List<List<String>> cycles = Utils.findComponentCycles(topology, name);
        if (!cycles.isEmpty()) {
            String err = String.format("Topology %s contains cycles in components \"%s\"", name,
                    cycles.stream().map(x -> String.join(",", x)).collect(Collectors.joining(" ; ")));
            throw new WrappedInvalidTopologyException(err);
        }
    }
}
