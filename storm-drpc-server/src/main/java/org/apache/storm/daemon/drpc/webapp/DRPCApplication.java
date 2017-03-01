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
package org.apache.storm.daemon.drpc.webapp;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.storm.daemon.drpc.DRPC;

@ApplicationPath("")
public class DRPCApplication extends Application {
    private static DRPC _drpc;
    private final Set<Object> singletons = new HashSet<Object>();
    
    public DRPCApplication() {
        singletons.add(new DRPCResource(_drpc));
        singletons.add(new DRPCExceptionMapper());
        singletons.add(new AuthorizationExceptionMapper());
    }
    
    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    public static void setup(DRPC drpc) {
        _drpc = drpc;
    }
}