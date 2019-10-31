/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import org.apache.storm.validation.ConfigValidation.Validator;
import org.apache.storm.validation.ConfigValidationAnnotations.CustomValidator;
import org.apache.storm.validation.ConfigValidationAnnotations.IsBoolean;
import org.apache.storm.validation.ConfigValidationAnnotations.IsInteger;
import org.apache.storm.validation.ConfigValidationAnnotations.IsMapEntryType;
import org.apache.storm.validation.ConfigValidationAnnotations.IsPositiveNumber;
import org.apache.storm.validation.ConfigValidationAnnotations.IsString;
import org.apache.storm.validation.NotConf;
import org.apache.storm.validation.Validated;

public class Configs implements Validated {
    /**
     * Required - chose the file type being consumed.
     * @deprecated please use {@link HdfsSpout#setReaderType(String)}
     */
    @Deprecated
    @IsString
    @CustomValidator(validatorClass = ReaderTypeValidator.class)
    public static final String READER_TYPE = "hdfsspout.reader.type";
    public static final String TEXT = "text";
    public static final String SEQ = "seq";
    /**
     * Required - HDFS name node.
     * @deprecated please use {@link HdfsSpout#setHdfsUri(String)}
     */
    @Deprecated
    @IsString
    public static final String HDFS_URI = "hdfsspout.hdfs";
    /**
     * Required - dir from which to read files.
     * @deprecated please use {@link HdfsSpout#setSourceDir(String)}
     */
    @Deprecated
    @IsString
    public static final String SOURCE_DIR = "hdfsspout.source.dir";
    /**
     * Required - completed files will be moved here.
     * @deprecated please use {@link HdfsSpout#setArchiveDir(String)}
     */
    @Deprecated
    @IsString
    public static final String ARCHIVE_DIR = "hdfsspout.archive.dir";
    /**
     * Required - unparsable files will be moved here.
     * @deprecated please use {@link HdfsSpout#setBadFilesDir(String)}
     */
    @Deprecated
    @IsString
    public static final String BAD_DIR = "hdfsspout.badfiles.dir";
    /**
     * Directory in which lock files will be created.
     * @deprecated please use {@link HdfsSpout#setLockDir(String)}
     */
    @Deprecated
    @IsString
    public static final String LOCK_DIR = "hdfsspout.lock.dir";
    /**
     * Commit after N records. 0 disables this.
     * @deprecated please use {@link HdfsSpout#setCommitFrequencyCount(int)}
     */
    @Deprecated
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String COMMIT_FREQ_COUNT = "hdfsspout.commit.count";
    /**
     * Commit after N secs. cannot be disabled.
     * @deprecated please use {@link HdfsSpout#setCommitFrequencySec(int)}
     */
    @Deprecated
    @IsInteger
    @IsPositiveNumber
    public static final String COMMIT_FREQ_SEC = "hdfsspout.commit.sec";
    /**
     * Max outstanding.
     * @deprecated please use {@link HdfsSpout#setMaxOutstanding(int)}
     */
    @Deprecated
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String MAX_OUTSTANDING = "hdfsspout.max.outstanding";
    /**
     * Lock timeout.
     * @deprecated please use {@link HdfsSpout#setLockTimeoutSec(int)}
     */
    @Deprecated
    @IsInteger
    @IsPositiveNumber
    public static final String LOCK_TIMEOUT = "hdfsspout.lock.timeout.sec";
    /**
     * If clocks on machines in the Storm cluster are in sync inactivity duration after which locks are considered
     * candidates for being reassigned to another spout.
     *
     * @deprecated please use {@link HdfsSpout#setClocksInSync(boolean)}
     */
    @Deprecated
    @IsBoolean
    public static final String CLOCKS_INSYNC = "hdfsspout.clocks.insync";
    /**
     * Ignore suffix.
     * @deprecated please use {@link HdfsSpout#setIgnoreSuffix(String)}
     */
    @Deprecated
    @IsString
    public static final String IGNORE_SUFFIX = "hdfsspout.ignore.suffix";
    /**
     * Filenames with this suffix in archive dir will be ignored by the Spout.
     */
    @NotConf
    public static final String DEFAULT_LOCK_DIR = ".lock";
    public static final int DEFAULT_COMMIT_FREQ_COUNT = 20000;
    public static final int DEFAULT_COMMIT_FREQ_SEC = 10;
    public static final int DEFAULT_MAX_OUTSTANDING = 10000;
    public static final int DEFAULT_LOCK_TIMEOUT = 5 * 60; // 5 min
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String DEFAULT_HDFS_CONFIG_KEY = "hdfs.config";

    public static class ReaderTypeValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            HdfsSpout.checkValidReader((String) o);
        }
    }
} // class Configs
