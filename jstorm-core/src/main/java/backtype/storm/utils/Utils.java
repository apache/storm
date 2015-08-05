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
package backtype.storm.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.StormTopology;
import backtype.storm.serialization.DefaultSerializationDelegate;
import backtype.storm.serialization.SerializationDelegate;
import clojure.lang.IFn;
import clojure.lang.RT;

import com.alibaba.jstorm.utils.LoadConf;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";

    private static SerializationDelegate serializationDelegate;

    static {
        Map conf = readStormConfig();
        serializationDelegate = getSerializationDelegate(conf);
    }

    public static Object newInstance(String klass) {
        try {
            Class c = Class.forName(klass);
            return c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object newInstance(String klass, Object ...params) {
        try {
            Class c = Class.forName(klass);
            Constructor[] constructors = c.getConstructors();
            boolean found = false;
            Constructor con = null;
            for (Constructor cons : constructors) {
                if (cons.getParameterTypes().length == params.length) {
                    con = cons;
                    break;
                }
            }
            
            if (con == null) {
                throw new RuntimeException("Cound not found the corresponding constructor, params=" + params.toString());
            } else {
                if (con.getParameterTypes().length == 0) {
                    return c.newInstance();
                } else {
                    return con.newInstance(params);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Go thrift gzip serializer
     * @param obj
     * @return
     */
    public static byte[] serialize(Object obj) {
        /**
         * @@@
         * JStorm disable the thrift.gz.serializer
         */
        //return serializationDelegate.serialize(obj);
        return javaSerialize(obj);
    }

    /**
     * Go thrift gzip serializer
     * @param obj
     * @return
     */
    public static <T> T deserialize(byte[] serialized, Class<T> clazz) {
        /**
         * @@@
         * JStorm disable the thrift.gz.serializer 
         */
        //return serializationDelegate.deserialize(serialized, clazz);
        return (T)javaDeserialize(serialized);
    }

    public static byte[] javaSerialize(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[])obj;
        }
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
    
    public static Object maybe_deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return javaDeserializeWithCL(data, null);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Deserialized with ClassLoader
     * @param serialized
     * @param loader
     * @return
     */
    public static Object javaDeserializeWithCL(byte[] serialized, URLClassLoader loader) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            Object ret = null;
            if (loader != null) {
                ClassLoaderObjectInputStream cis = new ClassLoaderObjectInputStream(loader, bis);
                ret = cis.readObject();
                cis.close();
            } else {
                ObjectInputStream ois = new ObjectInputStream(bis);
                ret = ois.readObject();
                ois.close();
            }
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static Object javaDeserialize(byte[] serialized) {
        return javaDeserializeWithCL(serialized, WorkerClassLoader.getInstance());
    }
    
    public static <T> T javaDeserialize(byte[] serialized, Class<T> clazz) {
        return (T)javaDeserializeWithCL(serialized, WorkerClassLoader.getInstance());
    }
    
    public static String to_json(Object m) {
        // return JSON.toJSONString(m);
        return JSONValue.toJSONString(m);
    }
    
    public static Object from_json(String json) {
        if (json == null) {
            return null;
        } else {
            // return JSON.parse(json);
            return JSONValue.parse(json);
        }
    }
    
    public static String toPrettyJsonString(Object obj) {
        Gson gson2 = new GsonBuilder().setPrettyPrinting().create();
        String ret = gson2.toJson(obj);
        
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

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        String ret = "";
        while(it.hasNext()) {
            ret = ret + it.next();
            if(it.hasNext()) {
                ret = ret + sep;
            }
        }
        return ret;
    }

    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Please directly use LoadConf.findResources(name);
     * @param name
     * @return
     */
    @Deprecated
    public static List<URL> findResources(String name) {
        return LoadConf.findResources(name);
    }

    /**
     * Please directly use LoadConf.findAndReadYaml(name);
     * @param name
     * @return
     */
    @Deprecated
    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        return LoadConf.findAndReadYaml(name, mustExist, false);
    }


    public static Map findAndReadConfigFile(String name) {
    	return LoadConf.findAndReadYaml(name, true, false);
    }

    public static Map readDefaultConfig() {
        return LoadConf.findAndReadYaml("defaults.yaml", true, false);
    }

    public static Map readCommandLineOpts() {
        Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if(commandOptions != null) {
            String[] configs = commandOptions.split(",");
            for (String config : configs) {
                config = URLDecoder.decode(config);
                String[] options = config.split("=", 2);
                if (options.length == 2) {
                    Object val = JSONValue.parse(options[1]);
                    if (val == null) {
                        val = options[1];
                    }
                    ret.put(options[0], val);
                }
            }
        }
        return ret;
    }

    
    public static void replaceLocalDir(Map<Object, Object> conf) {
        String stormHome = System.getProperty("jstorm.home");
        boolean isEmpty = StringUtils.isBlank(stormHome);
        
        Map<Object, Object> replaceMap = new HashMap<Object, Object>();
        
        for (Entry entry : conf.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof String) {
                if (StringUtils.isBlank((String) value) == true) {
                    continue;
                }
                
                String str = (String) value;
                if (isEmpty == true) {
                    // replace %JSTORM_HOME% as current directory
                    str = str.replace("%JSTORM_HOME%", ".");
                } else {
                    str = str.replace("%JSTORM_HOME%", stormHome);
                }
                
                replaceMap.put(key, str);
            }
        }
        
        conf.putAll(replaceMap);
    }
    
    public static Map loadDefinedConf(String confFile) {
        File file = new File(confFile);
        if (file.exists() == false) {
            return findAndReadConfigFile(confFile, true);
        }
        
        Yaml yaml = new Yaml();
        Map ret;
        try {
            ret = (Map) yaml.load(new FileReader(file));
        } catch (FileNotFoundException e) {
            ret = null;
        }
        if (ret == null)
            ret = new HashMap();
        
        return new HashMap(ret);
    }
    
    public static Map readStormConfig() {
        Map ret = readDefaultConfig();
        String confFile = System.getProperty("storm.conf.file");
        Map storm;
        if (StringUtils.isBlank(confFile) == true) {
            storm = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm = loadDefinedConf(confFile);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());
        
        replaceLocalDir(ret);
        return ret;
    }

    private static Object normalizeConf(Object conf) {
        if(conf==null) return new HashMap();
        if(conf instanceof Map) {
            Map confMap = new HashMap((Map) conf);
            for(Object key: confMap.keySet()) {
                Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if(conf instanceof List) {
            List confList =  new ArrayList((List) conf);
            for(int i=0; i<confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if(conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }
    
    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(normalizeConf(Utils.from_json(Utils.to_json(stormConf))));
    }
    
    public static Object getSetComponentObject(ComponentObject obj, URLClassLoader loader) {
        if (obj.getSetField() == ComponentObject._Fields.SERIALIZED_JAVA) {
            return javaDeserializeWithCL(obj.get_serialized_java(), loader);
        } else if (obj.getSetField() == ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }
    
    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }
    
    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }
    
    public static void downloadFromMaster(Map conf, String file, String localFile) throws IOException, TException {
        WritableByteChannel out = null;
        NimbusClient client = null;
        try {
            client = NimbusClient.getConfiguredClient(conf, 10 * 1000);
            String id = client.getClient().beginFileDownload(file);
            out = Channels.newChannel(new FileOutputStream(localFile));
            while (true) {
                ByteBuffer chunk = client.getClient().downloadChunk(id);
                int written = out.write(chunk);
                if (written == 0) {
                    client.getClient().finishFileDownload(id);
                    break;
                }
            }
        } finally {
            if (out != null)
                out.close();
            if (client != null)
                client.close();
        }
    }
	
    public static IFn loadClojureFn(String namespace, String name) {
        try {
          clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (Exception e) {
          //if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(namespace, name).deref();
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    public static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
        Map<V, K> ret = new HashMap<V, K>();
        for(K key: map.keySet()) {
            ret.put(map.get(key), key);
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String id) {
        if(topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if(topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if(topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static Integer getInt(Object o) {
      Integer result = getInt(o, null);
      if (null == result) {
        throw new IllegalArgumentException("Don't know how to convert null to int");
      }
      return result;
    }
    
    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        
        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            return Integer.parseInt(((String) o));
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
        }
    }
    
    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }
	
    public static class BoundedExponentialBackoffRetry extends ExponentialBackoffRetry {
        
        protected final int maxRetryInterval;
        
        public BoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries, int maxSleepTimeMs) {
            super(baseSleepTimeMs, maxRetries);
            this.maxRetryInterval = maxSleepTimeMs;
        }
        
        public int getMaxRetryInterval() {
            return this.maxRetryInterval;
        }
        
        @Override
        public int getSleepTimeMs(int count, long elapsedMs) {
            return Math.min(maxRetryInterval, super.getSleepTimeMs(count, elapsedMs));
        }
        
    }
    
    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        List<String> serverPorts = new ArrayList<String>();
        for(String zkServer: (List<String>) servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, String zkStr, Map conf, ZookeeperAuthInfo auth)
    {
        builder.connectString(zkStr)
            .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
            .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
            .retryPolicy(new StormBoundedExponentialBackoffRetry(
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if(auth!=null && auth.scheme!=null && auth.payload!=null) {
            builder = builder.authorization(auth.scheme, auth.payload);
        }
    }

    public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        return newCurator(conf, servers, port, "", auth);
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, root, auth);
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
        CuratorFramework ret = newCurator(conf, servers, port, auth);
        ret.start();
        return ret;
    }

    /**
     *
(defn integer-divided [sum num-pieces]
  (let [base (int (/ sum num-pieces))
        num-inc (mod sum num-pieces)
        num-bases (- num-pieces num-inc)]
    (if (= num-inc 0)
      {base num-bases}
      {base num-bases (inc base) num-inc}
      )))
     * @param sum
     * @param numPieces
     * @return
     */

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if(numInc!=0) {
            ret.put(base+1, numInc);
        }
        return ret;
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static void readAndLogStream(String prefix, InputStream in) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = r.readLine())!= null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error whiel trying to log stream", e);
        }
    }

    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        Throwable t = throwable;
        while(t != null) {
            if(klass.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way?
     * This only works when called from within Nimbus or a Supervisor process.
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map conf) {
        return null != System.getProperty("java.security.auth.login.config")
            || (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                && ! ((String)conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && ! ((String)conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    public static List<ACL> getWorkerACL(Map conf) {
        //This is a work around to an issue with ZK where a sasl super user is not super unless there is an open SASL ACL so we are trying to give the correct perms
        if (!isZkAuthenticationConfiguredTopology(conf)) {
            return null;
        }
        String stormZKUser = (String)conf.get(Config.STORM_ZOOKEEPER_SUPERACL);
        if (stormZKUser == null) {
           throw new IllegalArgumentException("Authentication is enabled but "+Config.STORM_ZOOKEEPER_SUPERACL+" is not set");
        }
        String[] split = stormZKUser.split(":",2);
        if (split.length != 2) {
          throw new IllegalArgumentException(Config.STORM_ZOOKEEPER_SUPERACL+" does not appear to be in the form scheme:acl, i.e. sasl:storm-user");
        }
        ArrayList<ACL> ret = new ArrayList<ACL>(ZooDefs.Ids.CREATOR_ALL_ACL);
        ret.add(new ACL(ZooDefs.Perms.ALL, new Id(split[0], split[1])));
        return ret;
    }

   public static String threadDump() {
       final StringBuilder dump = new StringBuilder();
       final java.lang.management.ThreadMXBean threadMXBean =  java.lang.management.ManagementFactory.getThreadMXBean();
       final java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
       for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
           dump.append('"');
           dump.append(threadInfo.getThreadName());
           dump.append("\" ");
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

    // Assumes caller is synchronizing
    private static SerializationDelegate getSerializationDelegate(Map stormConf) {
        String delegateClassName = (String)stormConf.get(Config.STORM_META_SERIALIZATION_DELEGATE);
        SerializationDelegate delegate;
        try {
            Class delegateClass = Class.forName(delegateClassName);
            delegate = (SerializationDelegate) delegateClass.newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        } catch (InstantiationException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        } catch (IllegalAccessException e) {
            LOG.error("Failed to construct serialization delegate, falling back to default", e);
            delegate = new DefaultSerializationDelegate();
        }
        delegate.prepare(stormConf);
        return delegate;
    }

  public static void handleUncaughtException(Throwable t) {
    if (t != null && t instanceof Error) {
      if (t instanceof OutOfMemoryError) {
        try {
          System.err.println("Halting due to Out Of Memory Error..." + Thread.currentThread().getName());
        } catch (Throwable err) {
          //Again we don't want to exit because of logging issues.
        }
        Runtime.getRuntime().halt(-1);
      } else {
        //Running in daemon mode, we would pass Error to calling thread.
        throw (Error) t;
      }
    }
  }


    
    public static List<String> tokenize_path(String path) {
        String[] toks = path.split("/");
        java.util.ArrayList<String> rtn = new ArrayList<String>();
        for (String str : toks) {
            if (!str.isEmpty()) {
                rtn.add(str);
            }
        }
        return rtn;
    }
    
    public static String toks_to_path(List<String> toks) {
        StringBuffer buff = new StringBuffer();
        buff.append("/");
        int size = toks.size();
        for (int i = 0; i < size; i++) {
            buff.append(toks.get(i));
            if (i < (size - 1)) {
                buff.append("/");
            }
            
        }
        return buff.toString();
    }
    
    public static String normalize_path(String path) {
        String rtn = toks_to_path(tokenize_path(path));
        return rtn;
    }
    
    public static String printStack() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nCurrent call stack:\n");
        StackTraceElement[] stackElements = Thread.currentThread().getStackTrace();
        for (int i = 2; i < stackElements.length; i++) {
            sb.append("\t").append(stackElements[i]).append("\n");
        }
        
        return sb.toString();
    }
    
    private static Map loadProperty(String prop) {
        Map ret = new HashMap<Object, Object>();
        Properties properties = new Properties();
        
        try {
            InputStream stream = new FileInputStream(prop);
            properties.load(stream);
            if (properties.size() == 0) {
                System.out.println("WARN: Config file is empty");
                return null;
            } else {
                ret.putAll(properties);
            }
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + prop);
            throw new RuntimeException(e.getMessage());
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException(e1.getMessage());
        }
        
        return ret;
    }
    
    private static Map loadYaml(String confPath) {
        Map ret = new HashMap<Object, Object>();
        Yaml yaml = new Yaml();
        
        try {
            InputStream stream = new FileInputStream(confPath);
            ret = (Map) yaml.load(stream);
            if (ret == null || ret.isEmpty() == true) {
                System.out.println("WARN: Config file is empty");
                return null;
            }
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + confPath);
            throw new RuntimeException("No config file");
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to read config file");
        }
        
        return ret;
    }
    
    public static Map loadConf(String arg) {
        Map ret = null;
        if (arg.endsWith("yaml")) {
            ret = loadYaml(arg);
        } else {
            ret = loadProperty(arg);
        }
        return ret;
    }

    public static String getVersion() {
        String ret = "";
        InputStream input = null;
        try {
            input =
                    Utils.class.getClassLoader().getResourceAsStream("version");
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String s = in.readLine();
			ret = s.trim();
			
			
        } catch (Exception e) {
            LOG.warn("Failed to get version", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    LOG.error("Failed to close the reader of RELEASE", e);
                }
            }
        }

        return ret;
    }

    public static void writeIntToByteArray(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (value & 0x000000FF);
        bytes[offset++] = (byte) ((value & 0x0000FF00) >> 8);
        bytes[offset++] = (byte) ((value & 0x00FF0000) >> 16);
        bytes[offset]   = (byte) ((value & 0xFF000000) >> 24);        
    }

    public static int readIntFromByteArray(byte[] bytes, int offset) {
        int ret = 0;
        ret = ret | (bytes[offset++] & 0x000000FF);
        ret = ret | ((bytes[offset++] << 8) & 0x0000FF00);
        ret = ret | ((bytes[offset++] << 16) & 0x00FF0000);
        ret = ret | ((bytes[offset]   << 24) & 0xFF000000);
        return ret;
    }
}
