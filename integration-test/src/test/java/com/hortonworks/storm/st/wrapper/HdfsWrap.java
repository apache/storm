/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.storm.st.wrapper;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;

/**
 * Created by Raghav Kumar Gautam on 6/24/16.
 */
public class HdfsWrap {
    private final FileSystem fileSystem;
    private static Logger log = LoggerFactory.getLogger(HdfsWrap.class);

    private HdfsWrap(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public static HdfsWrap getInstance(final FileSystem fileSystem) {
        return new HdfsWrap(fileSystem);
    }


    public void deleteDir(String hdfsDir) throws IOException {
        Path hdfsPath = new Path(hdfsDir);
        log.info("Deleting dir " + hdfsPath + " on filesystem " + fileSystem.getUri());
        fileSystem.delete(hdfsPath, true);
    }

    public void createDir(String hdfsDir) throws IOException {
        Path hdfsPath = new Path(hdfsDir);
        log.info("Creating dir " + hdfsPath + " on filesystem " + fileSystem.getUri());
        fileSystem.mkdirs(hdfsPath);
    }

    public SortedSet<String> readSortedRecords(String hdfsDir, final String delimiterRegex) throws IOException {
        final List<String> rawOutput = readDataFromHdfs(hdfsDir);
        return FluentIterable.from(rawOutput).transformAndConcat(new Function<String, Iterable<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable String s) {
                if (StringUtils.isEmpty(s)) {
                    return new ArrayList<>();
                }
                return Arrays.asList(s.split(delimiterRegex));
            }
        }).toSortedSet(Ordering.<String>natural());
    }

    private List<String> readDataFromHdfs(String hdfsPath) throws IOException {
        final File tempDir = new File(FileUtils.getTempDirectory(), "testHdfsSpoutBoltOutput");
        Assert.assertTrue(tempDir.mkdirs(), "temporary dir creation failed: " + tempDir);
        fileSystem.copyToLocalFile(new Path(hdfsPath), new Path(tempDir.getAbsolutePath()));
        List<String> outputData = new ArrayList<>();
        for (File oneOutputFile : FileUtils.listFiles(tempDir, new String[] {"txt"}, true)) {
            outputData.add(FileUtils.readFileToString(oneOutputFile, Charset.defaultCharset()));
        }
        FileUtils.deleteQuietly(tempDir);
        return outputData;
    }

    public void writeDataToHdfs(String data, String hdfsDir) throws IOException {
        final File tempFile1 = File.createTempFile("data", ".txt");
        FileUtils.writeStringToFile(tempFile1, data, Charset.defaultCharset());
        fileSystem.copyFromLocalFile(new Path(tempFile1.getAbsolutePath()), new Path(hdfsDir));
        FileUtils.deleteQuietly(tempFile1);
    }

    public void createHdfsDirWith777(String dir) throws IOException {
        final Path path = new Path(dir);
        createDir(dir);
        fileSystem.setPermission(path, FsPermission.valueOf("drwxrwxrwx"));
    }
}
