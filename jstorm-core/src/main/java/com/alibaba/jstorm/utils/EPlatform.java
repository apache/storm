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

public enum EPlatform {
    Any("any"), Linux("Linux"), Mac_OS("Mac OS"), Mac_OS_X("Mac OS X"), Windows("Windows"), OS2("OS/2"), Solaris("Solaris"), SunOS("SunOS"), MPEiX("MPE/iX"), HP_UX(
            "HP-UX"), AIX("AIX"), OS390("OS/390"), FreeBSD("FreeBSD"), Irix("Irix"), Digital_Unix("Digital Unix"), NetWare_411("NetWare"), OSF1("OSF1"), OpenVMS(
            "OpenVMS"), Others("Others");

    private EPlatform(String desc) {
        this.description = desc;
    }

    public String toString() {
        return description;
    }

    private String description;
}
