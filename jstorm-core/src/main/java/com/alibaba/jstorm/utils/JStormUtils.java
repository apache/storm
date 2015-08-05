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
package com.alibaba.jstorm.utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.management.ObjectName;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopDefaultKill;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;

/**
 * JStorm utility
 * 
 * @author yannian/Longda/Xin.Zhou/Xin.Li
 * 
 */
public class JStormUtils {
    private static final Logger LOG = LoggerFactory
            .getLogger(JStormUtils.class);

    public static long SIZE_1_K = 1024;
    public static long SIZE_1_M = SIZE_1_K * 1024;
    public static long SIZE_1_G = SIZE_1_M * 1024;
    public static long SIZE_1_T = SIZE_1_G * 1024;
    public static long SIZE_1_P = SIZE_1_T * 1024;

    public static final int MIN_1 = 60;
    public static final int MIN_30 = MIN_1 * 30;
    public static final int HOUR_1 = MIN_30 * 2;
    public static final int DAY_1 = HOUR_1 * 24;

    public static String getErrorInfo(String baseInfo, Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return baseInfo + "\r\n" + sw.toString() + "\r\n";
        } catch (Exception e2) {
            return baseInfo;
        }
    }

    public static String getErrorInfo(Throwable error) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            return sw.toString();
        } catch (Exception e1) {
            return "";
        }
    }

    /**
     * filter the map
     * 
     * @param filter
     * @param all
     * @return
     */
    public static <K, V> Map<K, V> select_keys_pred(Set<K> filter, Map<K, V> all) {
        Map<K, V> filterMap = new HashMap<K, V>();

        for (Entry<K, V> entry : all.entrySet()) {
            if (!filter.contains(entry.getKey())) {
                filterMap.put(entry.getKey(), entry.getValue());
            }
        }

        return filterMap;
    }

    public static byte[] barr(byte v) {
        byte[] byteArray = new byte[1];
        byteArray[0] = v;

        return byteArray;
    }

    public static byte[] barr(Short v) {
        byte[] byteArray = new byte[Short.SIZE / 8];
        for (int i = 0; i < byteArray.length; i++) {
            int off = (byteArray.length - 1 - i) * 8;
            byteArray[i] = (byte) ((v >> off) & 0xFF);
        }
        return byteArray;
    }

    public static byte[] barr(Integer v) {
        byte[] byteArray = new byte[Integer.SIZE / 8];
        for (int i = 0; i < byteArray.length; i++) {
            int off = (byteArray.length - 1 - i) * 8;
            byteArray[i] = (byte) ((v >> off) & 0xFF);
        }
        return byteArray;
    }

    // for test
    public static int byteToInt2(byte[] b) {

        int iOutcome = 0;
        byte bLoop;

        for (int i = 0; i < 4; i++) {
            bLoop = b[i];
            int off = (b.length - 1 - i) * 8;
            iOutcome += (bLoop & 0xFF) << off;

        }

        return iOutcome;
    }

    /**
     * LocalMode variable isn't clean, it make the JStormUtils ugly
     */
    public static boolean localMode = false;

    public static boolean isLocalMode() {
        return localMode;
    }

    public static void setLocalMode(boolean localMode) {
        JStormUtils.localMode = localMode;
    }

    public static void haltProcess(int val) {
        Runtime.getRuntime().halt(val);
    }

    public static void halt_process(int val, String msg) {
        LOG.info("Halting process: " + msg);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        if (localMode && val == 0) {
            // throw new RuntimeException(msg);
        } else {
            haltProcess(val);
        }
    }

    /**
     * "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
     * 
     * @param map
     * @return
     */
    public static <K, V> HashMap<V, List<K>> reverse_map(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Entry<K, V> entry : map.entrySet()) {
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
     * Gets the pid of this JVM, because Java doesn't provide a real way to do
     * this.
     * 
     * @return
     */
    public static String process_pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }

        return split[0];
    }

    public static void exec_command(String command) throws ExecuteException,
            IOException {
        String[] cmdlist = command.split(" ");
        CommandLine cmd = new CommandLine(cmdlist[0]);
        for (int i = 1; i < cmdlist.length; i++) {
            cmd.addArgument(cmdlist[i]);
        }

        DefaultExecutor exec = new DefaultExecutor();
        exec.execute(cmd);
    }

    /**
     * Extra dir from the jar to destdir
     * 
     * @param jarpath
     * @param dir
     * @param destdir
     */
    public static void extract_dir_from_jar(String jarpath, String dir,
            String destdir) {
        String cmd = "unzip -qq " + jarpath + " " + dir + "/** -d " + destdir;
        try {
            exec_command(cmd);
        } catch (Exception e) {
            LOG.warn("No " + dir + " from " + jarpath + " by cmd:" + cmd
                    + "!\n" + e.getMessage());
        }

    }

    public static void ensure_process_killed(Integer pid) {
        // in this function, just kill the process 5 times
        // make sure the process be killed definitely
        for (int i = 0; i < 5; i++) {
            try {
                exec_command("kill -9 " + pid);
                LOG.info("kill -9 process " + pid);
                sleepMs(100);
            } catch (ExecuteException e) {
                LOG.info("Error when trying to kill " + pid
                        + ". Process has been killed");
            } catch (Exception e) {
                LOG.info("Error when trying to kill " + pid + ".Exception ", e);
            }
        }
    }

    public static void process_killed(Integer pid) {
        try {
            exec_command("kill " + pid);
            LOG.info("kill process " + pid);
        } catch (ExecuteException e) {
            LOG.info("Error when trying to kill " + pid
                    + ". Process has been killed. ");
        } catch (Exception e) {
            LOG.info("Error when trying to kill " + pid + ".Exception ", e);
        }
    }

    public static void kill(Integer pid) {
        process_killed(pid);

        sleepMs(5 * 1000);

        ensure_process_killed(pid);
    }

    public static void kill_signal(Integer pid, String signal) {
        String cmd = "kill " + signal + " " + pid;
        try {
            exec_command(cmd);
            LOG.info(cmd);
        } catch (ExecuteException e) {
            LOG.info("Error when run " + cmd + ". Process has been killed. ");
        } catch (Exception e) {
            LOG.info("Error when run " + cmd + ". Exception ", e);
        }
    }

    /**
     * This function is only for linux
     * 
     * @param pid
     * @return
     */
    public static boolean isProcDead(String pid) {
        if (OSInfo.isLinux() == false) {
            return false;
        }

        String path = "/proc/" + pid;
        File file = new File(path);

        if (file.exists() == false) {
            LOG.info("Process " + pid + " is dead");
            return true;
        }

        return false;
    }

    public static Double getCpuUsage() {
        if (OSInfo.isLinux() == false) {
            return 0.0;
        }

        Double value = 0.0;
        String output = null;
        try {
            String pid = JStormUtils.process_pid();
            output = SystemOperation.exec("top -b -n 1 | grep " + pid);
            String subStr = output.substring(output.indexOf("S") + 1);
            for (int i = 0; i < subStr.length(); i++) {
                char ch = subStr.charAt(i);
                if (ch != ' ') {
                    subStr = subStr.substring(i);
                    break;
                }
            }
            String usedCpu = subStr.substring(0, subStr.indexOf(" "));
            value = Double.valueOf(usedCpu);
        } catch (Exception e) {
            LOG.warn("Failed to get cpu usage ratio.");
            if (output != null)
                LOG.warn("Output string is \"" + output + "\"");
            value = 0.0;
        }

        return value;
    }
    
    public static Double getMemUsage() {
    	if (OSInfo.isLinux() == true) {
    		try {
    			Double value = 0.0;
                String pid = JStormUtils.process_pid();
                String output = SystemOperation.exec("top -b -n 1 | grep " + pid);
                
                int m = 0;  
                String[] strArray = output.split(" ");  
                for (int i = 0; i < strArray.length; i++) {  
                    String info = strArray[i];  
                    if (info.trim().length() == 0){  
                        continue;  
                    }  
                    if(m == 5) {
                    	// memory
                        String unit = info.substring(info.length() - 1); 
                        
                        if(unit.equalsIgnoreCase("g")) {  
                            value =  Double.parseDouble(info.substring(0, info.length() - 1));
                            value *= 1000000000;
                        } else if(unit.equalsIgnoreCase("m")) {  
                        	value =  Double.parseDouble(info.substring(0, info.length() - 1)); 
                        	value *= 1000000;
                        } else {  
                        	value =  Double.parseDouble(info);  
                        } 
                        return value;
                    }  
                    if(m == 8) {
                    	// cpu usage
                          
                    }  
                    if(m == 9) {
                    	// memory ratio
                         
                    }  
                    m++;  
                }  
            } catch (Exception e) {
                LOG.warn("Failed to get memory usage .");

            }
    	}
    	
    	// this will be incorrect
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();    
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

        return Double.valueOf(memoryUsage.getUsed());
    }

    /**
     * If it is backend, please set resultHandler, such as
     * DefaultExecuteResultHandler If it is frontend,
     * ByteArrayOutputStream.toString get the result
     * 
     * This function don't care whether the command is successfully or not
     * 
     * @param command
     * @param environment
     * @param workDir
     * @param resultHandler
     * @return
     * @throws IOException
     */
    public static ByteArrayOutputStream launchProcess(String command,
            final Map environment, final String workDir,
            ExecuteResultHandler resultHandler) throws IOException {

        String[] cmdlist = command.split(" ");

        CommandLine cmd = new CommandLine(cmdlist[0]);
        for (String cmdItem : cmdlist) {
            if (StringUtils.isBlank(cmdItem) == false) {
                cmd.addArgument(cmdItem);
            }
        }

        DefaultExecutor executor = new DefaultExecutor();

        executor.setExitValue(0);
        if (StringUtils.isBlank(workDir) == false) {
            executor.setWorkingDirectory(new File(workDir));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        PumpStreamHandler streamHandler = new PumpStreamHandler(out, out);
        if (streamHandler != null) {
            executor.setStreamHandler(streamHandler);
        }

        try {
            if (resultHandler == null) {
                executor.execute(cmd, environment);
            } else {
                executor.execute(cmd, environment, resultHandler);
            }
        } catch (ExecuteException e) {

            // @@@@
            // failed to run command
        }

        return out;

    }

    protected static java.lang.Process launchProcess(final String[] cmdlist,
            final Map<String, String> environment) throws IOException {
        ArrayList<String> buff = new ArrayList<String>();
        for (String tok : cmdlist) {
            if (!tok.isEmpty()) {
                buff.add(tok);
            }
        }

        ProcessBuilder builder = new ProcessBuilder(buff);
        builder.redirectErrorStream(true);
        Map<String, String> process_evn = builder.environment();
        for (Entry<String, String> entry : environment.entrySet()) {
            process_evn.put(entry.getKey(), entry.getValue());
        }

        return builder.start();
    }

    /**
     * @@@ it should use DefaultExecutor to start a process, but some little
     *     problem have been found, such as exitCode/output string so still use
     *     the old method to start process
     * 
     * @param command
     * @param environment
     * @param backend
     * @return
     * @throws IOException
     */
    public static java.lang.Process launch_process(final String command,
            final Map<String, String> environment, boolean backend)
            throws IOException {

        if (backend == true) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    String[] cmdlist =
                            (new String("nohup " + command + " &")).split(" ");
                    try {
                        launchProcess(cmdlist, environment);
                    } catch (IOException e) {
                        LOG.error(
                                "Failed to run " + command + ":" + e.getCause(),
                                e);
                    }
                }
            }).start();
            return null;
        } else {
            String[] cmdlist = command.split(" ");
            return launchProcess(cmdlist, environment);
        }
    }

    public static String current_classpath() {
        return System.getProperty("java.class.path");
    }

    // public static String add_to_classpath(String classpath, String[] paths) {
    // for (String path : paths) {
    // classpath += ":" + path;
    // }
    // return classpath;
    // }

    public static String to_json(Map m) {
        return Utils.to_json(m);
    }

    public static Object from_json(String json) {
        return Utils.from_json(json);
    }

    public static <V> HashMap<V, Integer> multi_set(List<V> list) {
        HashMap<V, Integer> rtn = new HashMap<V, Integer>();
        for (V v : list) {
            int cnt = 1;
            if (rtn.containsKey(v)) {
                cnt += rtn.get(v);
            }
            rtn.put(v, cnt);
        }
        return rtn;
    }

    /**
     * 
     * if the list exist repeat string, return the repeated string
     * 
     * this function will be used to check wheter bolt or spout exist same id
     * 
     * @param sets
     * @return
     */
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

    /**
     * balance all T
     * 
     * @param <T>
     * @param splitup
     * @return
     */
    public static <T> List<T> interleave_all(List<List<T>> splitup) {
        ArrayList<T> rtn = new ArrayList<T>();
        int maxLength = 0;
        for (List<T> e : splitup) {
            int len = e.size();
            if (maxLength < len) {
                maxLength = len;
            }
        }

        for (int i = 0; i < maxLength; i++) {
            for (List<T> e : splitup) {
                if (e.size() > i) {
                    rtn.add(e.get(i));
                }
            }
        }

        return rtn;
    }

    public static Long bit_xor_vals(Object... vals) {
        Long rtn = 0l;
        for (Object n : vals) {
            rtn = bit_xor(rtn, n);
        }

        return rtn;
    }

    public static <T> Long bit_xor_vals(java.util.List<T> vals) {
        Long rtn = 0l;
        for (T n : vals) {
            rtn = bit_xor(rtn, n);
        }

        return rtn;
    }

    public static <T> Long bit_xor_vals_sets(java.util.Set<T> vals) {
        Long rtn = 0l;
        for (T n : vals) {
            rtn = bit_xor(rtn, n);
        }
        return rtn;
    }

    public static Long bit_xor(Object a, Object b) {
        Long rtn = 0l;

        if (a instanceof Long && b instanceof Long) {
            rtn = ((Long) a) ^ ((Long) b);
            return rtn;
        } else if (b instanceof Set) {
            Long bs = bit_xor_vals_sets((Set) b);
            return bit_xor(a, bs);
        } else if (a instanceof Set) {
            Long as = bit_xor_vals_sets((Set) a);
            return bit_xor(as, b);
        } else {
            Long ai = Long.parseLong(String.valueOf(a));
            Long bi = Long.parseLong(String.valueOf(b));
            rtn = ai ^ bi;
            return rtn;
        }

    }

    public static <V> List<V> mk_list(V... args) {
        ArrayList<V> rtn = new ArrayList<V>();
        for (V o : args) {
            rtn.add(o);
        }
        return rtn;
    }

    public static <V> List<V> mk_list(java.util.Set<V> args) {
        ArrayList<V> rtn = new ArrayList<V>();
        if (args != null) {
            for (V o : args) {
                rtn.add(o);
            }
        }
        return rtn;
    }

    public static <V> List<V> mk_list(Collection<V> args) {
        ArrayList<V> rtn = new ArrayList<V>();
        if (args != null) {
            for (V o : args) {
                rtn.add(o);
            }
        }
        return rtn;
    }

    public static <V> V[] mk_arr(V... args) {
        return args;
    }

    public static Long parseLong(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf((Integer) value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            throw new RuntimeException("Invalid value "
                    + o.getClass().getName() + " " + o);
        }
    }

    public static Double parseDouble(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Double.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Number value = (Integer) o;
            return value.doubleValue();
        } else if (o instanceof Long) {
            Number value = (Long) o;
            return value.doubleValue();
        } else if (o instanceof Double) {
            return (Double) o;
        } else {
            throw new RuntimeException("Invalid value "
                    + o.getClass().getName() + " " + o);
        }
    }

    public static Long parseLong(Object o, long defaultValue) {

        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Long.valueOf(String.valueOf(o));
        } else if (o instanceof Integer) {
            Integer value = (Integer) o;
            return Long.valueOf((Integer) value);
        } else if (o instanceof Long) {
            return (Long) o;
        } else {
            return defaultValue;
        }
    }

    public static Integer parseInt(Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof String) {
            return Integer.parseInt(String.valueOf(o));
        } else if (o instanceof Long) {
            long value = (Long) o;
            return Integer.valueOf((int) value);
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else {
            throw new RuntimeException("Invalid value "
                    + o.getClass().getName() + " " + o);
        }
    }

    public static Integer parseInt(Object o, int defaultValue) {
        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Integer.parseInt(String.valueOf(o));
        } else if (o instanceof Long) {
            long value = (Long) o;
            return Integer.valueOf((int) value);
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else {
            return defaultValue;
        }
    }

    public static boolean parseBoolean(Object o, boolean defaultValue) {
        if (o == null) {
            return defaultValue;
        }

        if (o instanceof String) {
            return Boolean.valueOf((String) o);
        } else if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            return defaultValue;
        }
    }

    public static <V> Set<V> listToSet(List<V> list) {
        if (list == null) {
            return null;
        }

        Set<V> set = new HashSet<V>();
        set.addAll(list);
        return set;
    }

    /**
     * Check whether the zipfile contain the resources
     * 
     * @param zipfile
     * @param resources
     * @return
     */
    public static boolean zipContainsDir(String zipfile, String resources) {

        Enumeration<? extends ZipEntry> entries = null;
        try {
            entries = (new ZipFile(zipfile)).entries();
            while (entries != null && entries.hasMoreElements()) {
                ZipEntry ze = entries.nextElement();
                String name = ze.getName();
                if (name.startsWith(resources + "/")) {
                    return true;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
            LOG.error(e + "zipContainsDir error");
        }

        return false;
    }

    public static Object add(Object oldValue, Object newValue) {
        if (oldValue == null) {
            return newValue;
        } else if (newValue == null) {
            return oldValue;
        }

        if (oldValue instanceof Long) {
            return (Long) oldValue + (Long) newValue;
        } else if (oldValue instanceof Double) {
            return (Double) oldValue + (Double) newValue;
        } else if (oldValue instanceof Integer) {
            return (Integer) oldValue + (Integer) newValue;
        } else if (oldValue instanceof Float) {
            return (Float) oldValue + (Float) newValue;
        } else if (oldValue instanceof Short) {
            return (Short) oldValue + (Short) newValue;
        } else if (oldValue instanceof BigInteger) {
            return ((BigInteger) oldValue).add((BigInteger) newValue);
        } else if (oldValue instanceof Number) {
            return ((Number) oldValue).doubleValue()
                    + ((Number) newValue).doubleValue();
        } else {
            return null;
        }
    }

    public static Object mergeList(List<Object> list) {
        Object ret = null;

        for (Object value : list) {
            ret = add(ret, value);
        }

        return ret;
    }

    public static List<Object> mergeList(List<Object> result, Object add) {
        if (add instanceof Collection) {
            for (Object o : (Collection) add) {
                result.add(o);
            }
        } else if (add instanceof Set) {
            for (Object o : (Collection) add) {
                result.add(o);
            }
        } else {
            result.add(add);
        }

        return result;
    }

    public static List<Object> distinctList(List<Object> input) {
        List<Object> retList = new ArrayList<Object>();

        for (Object object : input) {
            if (retList.contains(object)) {
                continue;
            } else {
                retList.add(object);
            }

        }

        return retList;
    }

    public static <K, V> Map<K, V> mergeMapList(List<Map<K, V>> list) {
        Map<K, V> ret = new HashMap<K, V>();

        for (Map<K, V> listEntry : list) {
            if (listEntry == null) {
                continue;
            }
            for (Entry<K, V> mapEntry : listEntry.entrySet()) {
                K key = mapEntry.getKey();
                V value = mapEntry.getValue();

                V retValue = (V) add(ret.get(key), value);

                ret.put(key, retValue);
            }
        }

        return ret;
    }

    public static String formatSimpleDouble(Double value) {
        try {
            java.text.DecimalFormat form =
                    new java.text.DecimalFormat("##0.000");
            String s = form.format(value);
            return s;
        } catch (Exception e) {
            return "0.000";
        }

    }

    public static double formatDoubleDecPoint2(Double value) {
        try {
            java.text.DecimalFormat form = new java.text.DecimalFormat("##.00");
            String s = form.format(value);
            return Double.valueOf(s);
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static double formatDoubleDecPoint4(Double value) {
        try {
            java.text.DecimalFormat form =
                    new java.text.DecimalFormat("###.0000");
            String s = form.format(value);
            return Double.valueOf(s);
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static Double convertToDouble(Object value) {
        Double ret;

        if (value == null) {
            ret = null;
        } else {
            if (value instanceof Integer) {
                ret = ((Integer) value).doubleValue();
            } else if (value instanceof Long) {
                ret = ((Long) value).doubleValue();
            } else if (value instanceof Float) {
                ret = ((Float) value).doubleValue();
            } else if (value instanceof Double) {
                ret = (Double) value;
            } else {
                ret = null;
            }
        }

        return ret;
    }

    public static String formatValue(Object value) {
        if (value == null) {
            return "0";
        }

        if (value instanceof Long) {
            return String.valueOf((Long) value);
        } else if (value instanceof Double) {
            return formatSimpleDouble((Double) value);
        } else {
            return String.valueOf(value);
        }
    }

    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {

        }
    }

    public static void sleepNs(int ns) {
        try {
            Thread.sleep(0, ns);
        } catch (InterruptedException e) {

        }
    }

    public static String HEXES = "0123456789ABCDEF";

    public static String toPrintableString(byte[] buf) {
        if (buf == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (byte b : buf) {
            if (index % 10 == 0) {
                sb.append("\n");
            }
            index++;

            sb.append(HEXES.charAt((b & 0xF0) >> 4));
            sb.append(HEXES.charAt((b & 0x0F)));
            sb.append(" ");

        }

        return sb.toString();
    }

    /**
     * @@@ Todo
     * 
     * @return
     */
    public static Long getPhysicMemorySize() {
        Object object;
        try {
            object =
                    ManagementFactory.getPlatformMBeanServer().getAttribute(
                            new ObjectName("java.lang", "type",
                                    "OperatingSystem"),
                            "TotalPhysicalMemorySize");
        } catch (Exception e) {
            LOG.warn("Failed to get system physical memory size,", e);
            return null;
        }

        Long ret = (Long) object;

        return ret;
    }

    public static String genLogName(String topology, Integer port) {
        return topology + "-worker-" + port + ".log";
    }

    // public static String getLog4jFileName(org.apache.log4j.Logger
    // log4jLogger) throws Exception{
    // Enumeration<org.apache.log4j.Appender> enumAppender =
    // log4jLogger.getAllAppenders();
    // org.apache.log4j.FileAppender fileAppender = null;
    // while (enumAppender.hasMoreElements()) {
    // org.apache.log4j.Appender appender = enumAppender.nextElement();
    // if (appender instanceof org.apache.log4j.FileAppender) {
    // fileAppender = (org.apache.log4j.FileAppender) appender;
    // break;
    // }
    // }
    // if (fileAppender != null) {
    // return fileAppender.getFile();
    //
    // }
    //
    // return null;
    // }

    public static String getLogFileName() {
        try {
            Logger rootLogger =
                    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            if (rootLogger instanceof ch.qos.logback.classic.Logger) {
                ch.qos.logback.classic.Logger logbackLogger =
                        (ch.qos.logback.classic.Logger) rootLogger;
                // Logger framework is Logback
                for (Iterator<ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent>> index =
                        logbackLogger.iteratorForAppenders(); index.hasNext();) {
                    ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                            index.next();
                    if (appender instanceof ch.qos.logback.core.FileAppender) {
                        ch.qos.logback.core.FileAppender fileAppender =
                                (ch.qos.logback.core.FileAppender) appender;
                        return fileAppender.getFile();
                    }
                }
            }
            // else if (rootLogger instanceof org.slf4j.impl.Log4jLoggerAdapter)
            // {
            // // slf4j-log4j
            // org.slf4j.impl.Log4jLoggerAdapter log4jAdapter =
            // (org.slf4j.impl.Log4jLoggerAdapter) rootLogger;
            // try {
            // Field field = log4jAdapter.getClass().getDeclaredField(
            // "logger");
            // field.setAccessible(true);
            // Object obj = field.get(log4jAdapter);
            // if (obj instanceof org.apache.log4j.spi.RootLogger) {
            // return getLog4jFileName((org.apache.log4j.spi.RootLogger) obj);
            // }
            // } catch (Exception e) {
            // e.printStackTrace();
            // }
            //
            // } else if (rootLogger instanceof org.apache.log4j.Logger) {
            // return getLog4jFileName((org.apache.log4j.Logger) rootLogger);
            // }

        } catch (Throwable e) {
            LOG.info("Failed to get root logger file name", e.getMessage());
            return null;
        }
        return null;
    }

    public static String getLogDir() {
        String file = JStormUtils.getLogFileName();
        if (file != null) {
            if (file.lastIndexOf(File.separator) < 0)
                return "";
            return file.substring(0, file.lastIndexOf(File.separator));
        }

        String stormHome = System.getProperty("jstorm.home");
        if (stormHome == null) {
            return "." + File.separator + "logs";
        } else {
            return stormHome + File.separator + "logs";
        }
    }

    public static void redirectOutput(String file) throws Exception {

        System.out.println("Redirect output to " + file);

        FileOutputStream workerOut = new FileOutputStream(new File(file));

        PrintStream ps =
                new PrintStream(new BufferedOutputStream(workerOut), true);
        System.setOut(ps);
        System.setErr(ps);

        LOG.info("Successfully redirect System.out to " + file);

    }

    public static RunnableCallback getDefaultKillfn() {

        return new AsyncLoopDefaultKill();
    }

    public static TreeMap<Integer, Integer> integer_divided(int sum,
            int num_pieces) {
        return Utils.integerDivided(sum, num_pieces);
    }

    public static <K, V> HashMap<K, V> filter_val(RunnableCallback fn,
            Map<K, V> amap) {
        HashMap<K, V> rtn = new HashMap<K, V>();

        for (Entry<K, V> entry : amap.entrySet()) {
            V value = entry.getValue();
            Object result = fn.execute(value);

            if (result == (Boolean) true) {
                rtn.put(entry.getKey(), value);
            }
        }
        return rtn;
    }

    public static List<Integer> getSupervisorPortList(Map conf) {
        List<Integer> portList =
                (List<Integer>) conf.get(Config.SUPERVISOR_SLOTS_PORTS);
        if (portList != null && portList.size() > 0) {
            return portList;
        }

        LOG.info("Generate port list through CPU cores and system memory size");

        double cpuWeight =
                ConfigExtension.getSupervisorSlotsPortCpuWeight(conf);
        int sysCpuNum = 4;
        try {
            sysCpuNum = Runtime.getRuntime().availableProcessors();
        } catch (Exception e) {
            LOG.info("Failed to get CPU cores, set cpu cores as 4");
            sysCpuNum = 4;
        }
        int cpuPortNum = (int) (sysCpuNum / cpuWeight);
        if (cpuPortNum < 1) {

            LOG.info("Invalid supervisor.slots.port.cpu.weight setting :"
                    + cpuWeight + ", cpu cores:" + sysCpuNum);
            cpuPortNum = 1;
        }

        int memPortNum = Integer.MAX_VALUE;
        Long physicalMemSize = JStormUtils.getPhysicMemorySize();
        if (physicalMemSize == null) {
            LOG.info("Failed to get memory size");
        } else {
            LOG.info("Get system memory size :" + physicalMemSize);
            long workerMemSize = ConfigExtension.getMemSizePerWorker(conf);
            memPortNum = (int) (physicalMemSize / workerMemSize);
            if (memPortNum < 1) {
                LOG.info("Invalide worker.memory.size setting:" + workerMemSize);
                memPortNum = 4;
            } else if (memPortNum < 4) {
                LOG.info("System memory is too small for jstorm");
                memPortNum = 4;
            }
        }

        int portNum = Math.min(cpuPortNum, memPortNum);
        if (portNum < 1) {
            portNum = 1;
        }

        int portBase = ConfigExtension.getSupervisorSlotsPortsBase(conf);
        portList = new ArrayList<Integer>();
        for (int i = 0; i < portNum; i++) {
            portList.add(portBase + i);
        }

        return portList;
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

    public static Object createDisruptorWaitStrategy(Map conf) {
        String waitStrategy =
                (String) conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY);
        Object ret;

        if (waitStrategy.indexOf("TimeoutBlockingWaitStrategy") != -1) {
            long timeout =
                    parseLong(conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT),
                            10);
            ret = Utils.newInstance(waitStrategy, timeout, TimeUnit.MILLISECONDS);
        } else {
            ret = Utils.newInstance(waitStrategy);
        }

        return ret;
    }
}
