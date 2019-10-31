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

package org.apache.storm.daemon.drpc;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.generated.DistributedRPCInvocations;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPCThrift implements DistributedRPC.Iface, DistributedRPCInvocations.Iface {
    private final DRPC drpc;

    public DRPCThrift(DRPC drpc) {
        this.drpc = drpc;
    }

    @Override
    public void result(String id, String result) throws AuthorizationException {
        drpc.returnResult(id, result);
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException {
        return drpc.fetchRequest(functionName);
    }

    @Override
    public void failRequest(String id) throws AuthorizationException {
        drpc.failRequest(id, null);
    }

    @Override
    public void failRequestV2(String id, DRPCExecutionException e) throws AuthorizationException {
        drpc.failRequest(id, e);
    }

    @Override
    public String execute(String functionName, String funcArgs)
        throws DRPCExecutionException, AuthorizationException {
        return drpc.executeBlocking(functionName, funcArgs);
    }
}
