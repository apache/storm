/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

/**
 * Implementation of VersionInfo that uses a properties file to get the VersionInfo.
 */
public interface IVersionInfo {
    /**
     * Get the version number of the build.
     * @return the version number of the build.
     */
    String getVersion();

    /**
     * Get the SCM revision number of the build.
     * @return the SCM revision number of the build.
     */
    String getRevision();

    /**
     * Get the SCM branch of the build.
     * @return the SCM branch of the build.
     */
    String getBranch();

    /**
     * Get the date/time the build happened.
     * @return the date/time of the build.
     */
    String getDate();

    /**
     * Get the name of the user that did the build.
     * @return the name of the user that did the build.
     */
    String getUser();

    /**
     * Get the full SCM URL for the build.
     * @return the SCM URL of the build.
     */
    String getUrl();

    /**
     * Get the checksum of the source.
     * @return the checksum of the source.
     */
    String getSrcChecksum();

    /**
     * Get a descriptive representation of the build meant for human consumption.
     * @return a descriptive representation of the build.
     */
    String getBuildVersion();
}
