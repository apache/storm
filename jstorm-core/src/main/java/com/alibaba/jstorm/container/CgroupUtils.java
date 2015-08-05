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
package com.alibaba.jstorm.container;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.SystemOperation;

public class CgroupUtils {

    public static final Logger LOG = LoggerFactory.getLogger(CgroupUtils.class);

    public static void deleteDir(String dir) {
        try {
            String cmd = "rmdir " + dir;
            SystemOperation.exec(cmd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("rm " + dir + " fail!", e);
        }
    }

    public static boolean fileExists(String dir) {
        File file = new File(dir);
        return file.exists();
    }

    public static boolean dirExists(String dir) {
        File file = new File(dir);
        return file.isDirectory();
    }

    public static Set<SubSystemType> analyse(String str) {
        Set<SubSystemType> result = new HashSet<SubSystemType>();
        String[] subSystems = str.split(",");
        for (String subSystem : subSystems) {
            SubSystemType type = SubSystemType.getSubSystem(subSystem);
            if (type != null)
                result.add(type);
        }
        return result;
    }

    public static String reAnalyse(Set<SubSystemType> subSystems) {
        StringBuilder sb = new StringBuilder();
        if (subSystems.size() == 0)
            return sb.toString();
        for (SubSystemType type : subSystems) {
            sb.append(type.name()).append(",");
        }
        return sb.toString().substring(0, sb.length() - 1);
    }

    public static boolean enabled() {
        return CgroupUtils.fileExists(Constants.CGROUP_STATUS_FILE);
    }

    public static List<String> readFileByLine(String fileDir)
            throws IOException {
        List<String> result = new ArrayList<String>();
        FileReader fileReader = null;
        BufferedReader reader = null;
        try {
            File file = new File(fileDir);
            fileReader = new FileReader(file);
            reader = new BufferedReader(fileReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                result.add(tempString);
            }
        } finally {
            CgroupUtils.close(fileReader, reader);
        }
        return result;
    }

    public static void writeFileByLine(String fileDir, List<String> strings)
            throws IOException {
        FileWriter writer = null;
        BufferedWriter bw = null;
        try {
            File file = new File(fileDir);
            if (!file.exists()) {
                LOG.error(fileDir + " is no existed");
                return;
            }
            writer = new FileWriter(file, true);
            bw = new BufferedWriter(writer);
            for (String string : strings) {
                bw.write(string);
                bw.newLine();
                bw.flush();
            }
        } finally {
            CgroupUtils.close(writer, bw);
        }
    }

    public static void writeFileByLine(String fileDir, String string)
            throws IOException {
        FileWriter writer = null;
        BufferedWriter bw = null;
        try {
            File file = new File(fileDir);
            if (!file.exists()) {
                LOG.error(fileDir + " is no existed");
                return;
            }
            writer = new FileWriter(file, true);
            bw = new BufferedWriter(writer);
            bw.write(string);
            bw.newLine();
            bw.flush();

        } finally {
            CgroupUtils.close(writer, bw);
        }
    }

    public static void close(FileReader reader, BufferedReader br) {
        try {
            if (reader != null)
                reader.close();
            if (br != null)
                br.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block

        }
    }

    public static void close(FileWriter writer, BufferedWriter bw) {
        try {
            if (writer != null)
                writer.close();
            if (bw != null)
                bw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block

        }
    }

    public static void sleep(long s) {
        try {
            Thread.sleep(s);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
        }
    }
}
