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

import java.util.concurrent.Semaphore;
import org.apache.storm.generated.DRPCExceptionType;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.WrappedDRPCExecutionException;

public class BlockingOutstandingRequest extends OutstandingRequest {
    public static final RequestFactory<BlockingOutstandingRequest> FACTORY = BlockingOutstandingRequest::new;
    private Semaphore sem;
    private volatile String result = null;
    private volatile DRPCExecutionException drpcExecutionException = null;

    public BlockingOutstandingRequest(String function, DRPCRequest req) {
        super(function, req);
        sem = new Semaphore(0);
    }

    public String getResult() throws DRPCExecutionException {
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            //Ignored
        }

        if (result != null) {
            return result;
        }

        if (drpcExecutionException == null) {
            drpcExecutionException = new WrappedDRPCExecutionException("Internal Error: No Result and No Exception");
            drpcExecutionException.set_type(DRPCExceptionType.INTERNAL_ERROR);
        }
        throw drpcExecutionException;
    }

    @Override
    public void returnResult(String result) {
        this.result = result;
        sem.release();
    }

    @Override
    public void fail(DRPCExecutionException e) {
        drpcExecutionException = e;
        sem.release();
    }
}
