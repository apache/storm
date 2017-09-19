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
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.NimbusBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.serialization.DefaultSerializationDelegate;
import org.apache.storm.serialization.SerializationDelegate;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.google.common.annotations.VisibleForTesting;

import javax.security.auth.Subject;

public class Utils {
    public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";
    private static final Set<Class> defaultAllowedExceptions = new HashSet<>();
    public static final String FILE_PATH_SEPARATOR = System.getProperty("file.separator");
    private static final List<String> LOCALHOST_ADDRESSES = Lists.newArrayList("localhost", "127.0.0.1", "0:0:0:0:0:0:0:1");

    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();

    private static ClassLoader cl = null;
    private static Map<String, Object> localConf;
    static SerializationDelegate serializationDelegate;

    static {
        localConf = readStormConfig();
        serializationDelegate = getSerializationDelegate(localConf);
    }

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static Utils _instance = new Utils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
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
        } catch(IOException e) {
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
                if(confFileEmpty)
                    throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
                else
                    throw new RuntimeException("Could not find config file on classpath " + name);
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
            LOG.debug("Using "+configFilePath+" from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    public static Map<String, Object> readDefaultConfig() {
        return findAndReadConfigFile("defaults.yaml", true);
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
                config = URLDecoder.decode(config);
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
     * Adds the user supplied function as a shutdown hook for cleanup.
     * Also adds a function that sleeps for a second and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithForceKillIn1Sec (Runnable func) {
        Runnable sleepKill = new Runnable() {
            @Override
            public void run() {
                try {
                    Time.sleepSecs(1);
                    LOG.warn("Forceing Halt...");
                    Runtime.getRuntime().halt(20);
                } catch (Exception e) {
                    LOG.warn("Exception in the ShutDownHook", e);
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(func));
        Runtime.getRuntime().addShutdownHook(new Thread(sleepKill));
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    /**
     * Creates a thread that calls the given code repeatedly, sleeping for an
     * interval of seconds equal to the return value of the previous call.
     *
     * The given afn may be a callable that returns the number of seconds to
     * sleep, or it may be a Callable that returns another Callable that in turn
     * returns the number of seconds to sleep. In the latter case isFactory.
     *
     * @param afn the code to call on each iteration
     * @param isDaemon whether the new thread should be a daemon thread
     * @param eh code to call when afn throws an exception
     * @param priority the new thread's priority
     * @param isFactory whether afn returns a callable instead of sleep seconds
     * @param startImmediately whether to start the thread before returning
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn,
            boolean isDaemon, final Thread.UncaughtExceptionHandler eh,
            int priority, final boolean isFactory, boolean startImmediately,
            String threadName) {
        SmartThread thread = new SmartThread(new Runnable() {
            public void run() {
                Object s;
                try {
                    Callable fn = isFactory ? (Callable) afn.call() : afn;
                    while ((s = fn.call()) instanceof Long) {
                        Time.sleepSecs((Long) s);
                    }
                } catch (Throwable t) {
                    if (exceptionCauseIsInstanceOf(
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
                    exitProcess(1, "Async loop died!");
                }
            });
        }
        thread.setDaemon(isDaemon);
        thread.setPriority(priority);
        if (threadName != null && !threadName.isEmpty()) {
            thread.setName(thread.getName() +"-"+ threadName);
        }
        if (startImmediately) {
            thread.start();
        }
        return thread;
    }

    /**
     * Convenience method used when only the function and name suffix are given.
     * @param afn the code to call on each iteration
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn, String threadName, final Thread.UncaughtExceptionHandler eh) {
        return asyncLoop(afn, false, eh, Thread.NORM_PRIORITY, false, true,
                threadName);
    }

    /**
     * Convenience method used when only the function is given.
     * @param afn the code to call on each iteration
     * @return the newly created thread
     */
    public static SmartThread asyncLoop(final Callable afn) {
        return asyncLoop(afn, false, null, Thread.NORM_PRIORITY, false, true,
                null);
    }

    /**
     * Checks if a throwable is an instance of a particular class
     * @param klass The class you're expecting
     * @param throwable The throwable you expect to be an instance of klass
     * @return true if throwable is instance of klass, false otherwise.
     */
    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        Throwable t = throwable;
        while (t != null) {
            if (klass.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    public static RuntimeException wrapInRuntime(Exception e){
        if (e instanceof RuntimeException){
            return (RuntimeException)e;
        } else {
            return new RuntimeException(e);
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    /**
     * Gets the storm.local.hostname value, or tries to figure out the local hostname
     * if it is not set in the config.
     * @return a string representation of the hostname.
     */
    public static String hostname() throws UnknownHostException {
        return _instance.hostnameImpl();
    }


    public static String localHostname () throws UnknownHostException {
        return _instance.localHostnameImpl();
    }

    public static void exitProcess (int val, String msg) {
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
            return (T)ret;
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
        while(it.hasNext()) {
            ret.append(it.next());
            if(it.hasNext()) {
                ret.append(sep);
            }
        }
        return ret.toString();
    }

    public static List<ACL> getWorkerACL(Map<String, Object> conf) {
        //This is a work around to an issue with ZK where a sasl super user is not super unless there is an open SASL ACL so we are trying to give the correct perms
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        String stormZKUser = (String)conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
            throw new IllegalArgumentException("Authentication is enabled but " + Config.STORM_ZOOKEEPER_SUPERACL + " is not set");
        }
        String[] split = stormZKUser.split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(Config.STORM_ZOOKEEPER_SUPERACL + " does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        ArrayList<ACL> ret = new ArrayList<ACL>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(new ACL(ZooDefs.Perms.ALL, new Id(split[0], split[1])));
        return ret;
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map<String, Object> conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && !((String)conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    public static void handleUncaughtException(Throwable t) {
        handleUncaughtException(t, defaultAllowedExceptions);
    }

    public static void handleUncaughtException(Throwable t, Set<Class> allowedExceptions) {
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

        if(allowedExceptions.contains(t.getClass())) {
            LOG.info("Swallowing {} {}", t.getClass(), t);
            return;
        }

        //Running in daemon mode, we would pass Error to calling thread.
        throw new Error(t);
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
        if(des == null) {
            des = new TDeserializer();
            threadDes.set(des);
        }
        return des;
    }

    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static UptimeComputer makeUptimeComputer() {
        return _instance.makeUptimeComputerImpl();
    }

    /**
     * "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
     *
     * Example usage in java:
     *  Map<Integer, String> tasks;
     *  Map<String, List<Integer>> componentTasks = Utils.reverse_map(tasks);
     *
     * The order of he resulting list values depends on the ordering properties
     * of the Map passed in. The caller is responsible for passing an ordered
     * map if they expect the result to be consistently ordered as well.
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
     * Deletes a file or directory and its contents if it exists. Does not
     * complain if the input is null or does not exist.
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
     * Creates an instance of the pluggable SerializationDelegate or falls back to
     * DefaultSerializationDelegate if something goes wrong.
     * @param topoConf The config from which to pull the name of the pluggable class.
     * @return an instance of the class specified by storm.meta.serialization.delegate
     */
    private static SerializationDelegate getSerializationDelegate(Map<String, Object> topoConf) {
        String delegateClassName = (String)topoConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
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

    public static GlobalStreamId getGlobalStreamId(String streamId, String componentId) {
        if (componentId == null) {
            return new GlobalStreamId(streamId, DEFAULT_STREAM_ID);
        }
        return new GlobalStreamId(streamId, componentId);
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
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against Integer.MAX_VALUE(0x7fffffff) which
     * is not its absolutely value.
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & Integer.MAX_VALUE;
    }

    /**
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
            return (Map<String,Object>)ret;
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new map with a string value in the map replaced with an
     * equivalently-lengthed string of '#'.  (If the object is not a string
     * to string will be called on it and replaced)
     * @param m The map that a value will be redacted from
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

    public static void setupDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread thread, Throwable thrown) {
                    try {
                        handleUncaughtException(thrown);
                    } catch (Error err) {
                        LOG.error("Received error in main thread.. terminating server...", err);
                        Runtime.getRuntime().exit(-2);
                    }
                }
            });
    }

    public static Map<String, Object> findAndReadConfigFile(String name) {
        return findAndReadConfigFile(name, true);
    }

    /**
     * "[[:a 1] [:b 1] [:c 2]} -> {1 [:a :b] 2 :c}"
     * Reverses an assoc-list style Map like reverseMap(Map...)
     *
     * @param listSeq to reverse
     * @return a reversed map
     */
    public static HashMap reverseMap(List listSeq) {
        HashMap<Object, List<Object>> rtn = new HashMap();
        if (listSeq == null) {
            return rtn;
        }
        for (Object entry : listSeq) {
            List listEntry = (List) entry;
            Object key = listEntry.get(0);
            Object val = listEntry.get(1);
            List list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<Object>();
                rtn.put(val, list);
            }
            list.add(key);
        }
        return rtn;
    }

    /**
     * parses the arguments to extract jvm heap memory size in MB.
     * @param input
     * @param defaultValue
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
                    int value = Integer.parseInt(m.group(1));
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
                    Double result =  value * unit / 1024.0 / 1024.0;
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
            List<Object> confList =  new ArrayList<>((Collection<Object>) obj);
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

    public static boolean isValidConf(Map<String, Object> topoConf) {
        return normalizeConf(topoConf).equals(normalizeConf((Map<String, Object>) JSONValue.parse(JSONValue.toJSONString(topoConf))));
    }

    public static TopologyInfo getTopologyInfo(String name, String asUser, Map<String, Object> topoConf) {
        try (NimbusClient client = NimbusClient.getConfiguredClientAs(topoConf, asUser)) {
            String topologyId = getTopologyId(name, client.getClient());
            if (null != topologyId) {
                return client.getClient().getTopologyInfo(topologyId);
            }
            return null;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTopologyId(String name, Nimbus.Iface client) {
        try {
            ClusterSummary summary = client.getClusterInfo();
            for(TopologySummary s : summary.get_topologies()) {
                if(s.get_name().equals(name)) {
                    return s.get_id();
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Validate topology blobstore map.
     * @param topoConf Topology configuration
     * @throws InvalidTopologyException
     * @throws AuthorizationException
     */
    public static void validateTopologyBlobStoreMap(Map<String, Object> topoConf) throws InvalidTopologyException, AuthorizationException {
        try (NimbusBlobStore client = new NimbusBlobStore()) {
            client.prepare(topoConf);
            validateTopologyBlobStoreMap(topoConf, client);
        }
    }

    /**
     * Validate topology blobstore map.
     * @param topoConf Topology configuration
     * @param client The NimbusBlobStore client. It must call prepare() before being used here.
     * @throws InvalidTopologyException
     * @throws AuthorizationException
     */
    public static void validateTopologyBlobStoreMap(Map<String, Object> topoConf, NimbusBlobStore client)
            throws InvalidTopologyException, AuthorizationException {
        Map<String, Object> blobStoreMap = (Map<String, Object>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        if (blobStoreMap != null) {
            for (String key : blobStoreMap.keySet()) {
                // try to get BlobMeta
                // This will check if the key exists and if the subject has authorization
                try {
                    client.getBlobMeta(key);
                } catch (KeyNotFoundException keyNotFound) {
                    // wrap KeyNotFoundException in an InvalidTopologyException
                    throw new InvalidTopologyException("Key not found: " + keyNotFound.get_msg());
                }
            }
        }
    }

    /**
     * Validate topology blobstore map.
     * @param topoConf Topology configuration
     * @param blobStore The BlobStore
     * @throws InvalidTopologyException
     * @throws AuthorizationException
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
                    throw new InvalidTopologyException("Key not found: " + keyNotFound.get_msg());
                }
            }
        }
    }

    /**
     * Gets some information, including stack trace, for a running thread.
     * @return A human-readable string of the dump.
     */
    public static String threadDump() {
        final StringBuilder dump = new StringBuilder();
        final java.lang.management.ThreadMXBean threadMXBean =  ManagementFactory.getThreadMXBean();
        final java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            dump.append("\n   lock: ");
            dump.append(threadInfo.getLockName());
            dump.append(" owner: ");
            dump.append(threadInfo.getLockOwnerName());
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    public static long getVersionFromBlobVersionFile(File versionFile) {
        long currentVersion = 0;
        if (versionFile.exists() && !(versionFile.isDirectory())) {
            try (BufferedReader br = new BufferedReader(new FileReader(versionFile))) {
                String line = br.readLine();
                currentVersion = Long.parseLong(line);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return currentVersion;
        } else {
            return -1;
        }
    }

    public static boolean checkDirExists(String dir) {
        File file = new File(dir);
        return file.isDirectory();
    }

    /**
     * Return a new instance of a pluggable specified in the conf.
     * @param conf The conf to read from.
     * @param configKey The key pointing to the pluggable class
     * @return an instance of the class or null if it is not specified.
     */
    public static Object getConfiguredClass(Map<String, Object> conf, Object configKey) {
        if (conf.containsKey(configKey)) {
            return ReflectionUtils.newInstance((String)conf.get(configKey));
        }
        return null;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way?
     * This only works when called from within Nimbus or a Supervisor process.
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map<String, Object> conf) {
        return null != System.getProperty("java.security.auth.login.config")
                || (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                && !((String)conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
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

    public static double nullToZero (Double v) {
        return (v != null ? v : 0);
    }

    /**
     * a or b the first one that is not null
     * @param a something
     * @param b something else
     * @return a or b the first one that is not null
     */
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
            ret.put(base+1, numInc);
        }
        return ret;
    }

    /**
     * Fills up chunks out of a collection (given a maximum amount of chunks)
     *
     * i.e. partitionFixed(5, [1,2,3]) -> [[1,2,3]]
     *      partitionFixed(5, [1..9]) -> [[1,2], [3,4], [5,6], [7,8], [9]]
     *      partitionFixed(3, [1..10]) -> [[1,2,3,4], [5,6,7], [8,9,10]]
     * @param maxNumChunks the maximum number of chunks to return
     * @param coll the collection to be chunked up
     * @return a list of the chunks, which are themselves lists.
     */
    public static <T> List<List<T>> partitionFixed(int maxNumChunks, Collection<T> coll) {
        List<List<T>> ret = new ArrayList<>();

        if(maxNumChunks == 0 || coll == null) {
            return ret;
        }

        Map<Integer, Integer> parts = integerDivided(coll.size(), maxNumChunks);

        // Keys sorted in descending order
        List<Integer> sortedKeys = new ArrayList<Integer>(parts.keySet());
        Collections.sort(sortedKeys, Collections.reverseOrder());


        Iterator<T> it = coll.iterator();
        for(Integer chunkSize : sortedKeys) {
            if(!it.hasNext()) { break; }
            Integer times = parts.get(chunkSize);
            for(int i = 0; i < times; i++) {
                if(!it.hasNext()) { break; }
                List<T> chunkList = new ArrayList<>();
                for(int j = 0; j < chunkSize; j++) {
                    if(!it.hasNext()) { break; }
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
        } catch(Exception ex) {
            LOG.error("Failed to read yaml file.", ex);
        }
        return null;
    }

    /**
     * Gets an available port. Consider if it is possible to pass port 0 to the
     * server instead of using this method, since there is no guarantee that the
     * port returned by this method will remain free.
     *
     * @param preferredPort
     * @return The preferred port if available, or a random available port
     */
    public static int getAvailablePort(int preferredPort) {
        int localPort = -1;
        try (ServerSocket socket = new ServerSocket(preferredPort)) {
            localPort = socket.getLocalPort();
        } catch(IOException exp) {
            if (preferredPort > 0) {
                return getAvailablePort(0);
            }
        }
        return localPort;
    }

    /**
     * Shortcut to calling {@link #getAvailablePort(int) } with 0 as the preferred port
     * @return A random available port
     */
    public static int getAvailablePort() {
        return getAvailablePort(0);
    }

    /**
     * Find the first item of coll for which pred.test(...) returns true.
     * @param pred The IPredicate to test for
     * @param coll The Collection of items to search through.
     * @return The first matching value in coll, or null if nothing matches.
     */
    public static <T> T findOne (IPredicate<T> pred, Collection<T> coll) {
        if(coll == null) {
            return null;
        }
        for(T elem : coll) {
            if (pred.test(elem)) {
                return elem;
            }
        }
        return null;
    }

    public static <T, U> T findOne (IPredicate<T> pred, Map<U, T> map) {
        if (map == null) {
            return null;
        }
        return findOne(pred, (Set<T>) map.entrySet());
    }

    // Non-static impl methods exist for mocking purposes.
    protected void forceDeleteImpl(String path) throws IOException {
        LOG.debug("Deleting path {}", path);
        if (checkFileExists(path)) {
            try {
                FileUtils.forceDelete(new File(path));
            } catch (FileNotFoundException ignored) {}
        }
    }

    // Non-static impl methods exist for mocking purposes.
    public UptimeComputer makeUptimeComputerImpl() {
        return new UptimeComputer();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String localHostnameImpl () throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    private static String memoizedLocalHostnameString = null;

    public static String memoizedLocalHostname () throws UnknownHostException {
        if (memoizedLocalHostnameString == null) {
            memoizedLocalHostnameString = localHostname();
        }
        return memoizedLocalHostnameString;
    }

    // Non-static impl methods exist for mocking purposes.
    protected String hostnameImpl () throws UnknownHostException  {
        if (localConf == null) {
            return memoizedLocalHostname();
        }
        Object hostnameString = localConf.get(Config.STORM_LOCAL_HOSTNAME);
        if (hostnameString == null || hostnameString.equals("")) {
            return memoizedLocalHostname();
        }
        return (String)hostnameString;
    }

    /**
     * A thread that can answer if it is sleeping in the case of simulated time.
     * This class is not useful when simulated time is not being used.
     */
    public static class SmartThread extends Thread {
        public boolean isSleeping() {
            return Time.isThreadWaiting(this);
        }
        public SmartThread(Runnable r) {
            super(r);
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

    /**
     * Add version information to the given topology
     * @param topology the topology being submitted (MIGHT BE MODIFIED)
     * @return topology
     */
    public static StormTopology addVersions(StormTopology topology) {
        String stormVersion = VersionInfo.getVersion();
        if (stormVersion != null && 
                !"Unknown".equalsIgnoreCase(stormVersion) && 
                !topology.is_set_storm_version()) {
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
     * @param conf what to read it out of
     * @param currentCP the current classpath for this version of storm (not included in the conf, but returned by this)
     * @return the map
     */
    public static NavigableMap<SimpleVersion, List<String>> getConfiguredClasspathVersions(Map<String, Object> conf, List<String> currentCP) {
        TreeMap<SimpleVersion, List<String>> ret = new TreeMap<>();
        Map<String, String> fromConf = (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry: fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), Arrays.asList(entry.getValue().split(File.pathSeparator)));
        }
        ret.put(VersionInfo.OUR_VERSION, currentCP);
        return ret;
    }
    
    /**
     * Get a map of version to worker main from the conf Config.SUPERVISOR_WORKER_VERSION_MAIN_MAP
     * @param conf what to read it out of
     * @return the map
     */
    public static NavigableMap<SimpleVersion, String> getConfiguredWorkerMainVersions(Map<String, Object> conf) {
        TreeMap<SimpleVersion, String> ret = new TreeMap<>();
        Map<String, String> fromConf = (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_MAIN_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry: fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), entry.getValue());
        }

        ret.put(VersionInfo.OUR_VERSION, "org.apache.storm.daemon.worker.Worker");
        return ret;
    }
    
    
    /**
     * Get a map of version to worker log writer from the conf Config.SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP
     * @param conf what to read it out of
     * @return the map
     */
    public static NavigableMap<SimpleVersion, String> getConfiguredWorkerLogWriterVersions(Map<String, Object> conf) {
        TreeMap<SimpleVersion, String> ret = new TreeMap<>();
        Map<String, String> fromConf = (Map<String, String>) conf.getOrDefault(Config.SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP, Collections.emptyMap());
        for (Map.Entry<String, String> entry: fromConf.entrySet()) {
            ret.put(new SimpleVersion(entry.getKey()), entry.getValue());
        }

        ret.put(VersionInfo.OUR_VERSION, "org.apache.storm.LogWriter");
        return ret;
    }
    
    
    public static <T> T getCompatibleVersion(NavigableMap<SimpleVersion, T> versionedMap, SimpleVersion desiredVersion, String what, T defaultValue) {
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
        for (String part: cp) {
            File f = new File(part);
            if (f.isDirectory()) {
                if (defaultsConf == null) {
                    defaultsConf = readConfIgnoreNotFound(yaml, new File(f, "defaults.yaml"));
                }
                
                if (stormConf == null) {
                    stormConf = readConfIgnoreNotFound(yaml, new File(f, "storm.yaml"));
                }
            } else {
                //Lets assume it is a jar file
                try (JarFile jarFile = new JarFile(f)) {
                    Enumeration<JarEntry> jarEnums = jarFile.entries();
                    while (jarEnums.hasMoreElements()) {
                        JarEntry entry = jarEnums.nextElement();
                        if (!entry.isDirectory()) {
                            if (defaultsConf == null && entry.getName().equals("defaults.yaml")) {
                                try (InputStream in = jarFile.getInputStream(entry)) {
                                    defaultsConf = (Map<String, Object>) yaml.load(new InputStreamReader(in));
                                }
                            }
                            
                            if (stormConf == null && entry.getName().equals("storm.yaml")) {
                                try (InputStream in = jarFile.getInputStream(entry)) {
                                    stormConf = (Map<String, Object>) yaml.load(new InputStreamReader(in));
                                }
                            }
                        }
                    }
                }
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
}
