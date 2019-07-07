/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.transactional;

import java.math.BigInteger;

/**
 * This is dead code. It is retained to avoid breaking Kryo registration order. 
 */
@Deprecated
public class TransactionAttempt {
    BigInteger txid;
    long attemptId;


    // for kryo compatibility
    public TransactionAttempt() {

    }

    public TransactionAttempt(BigInteger txid, long attemptId) {
        this.txid = txid;
        this.attemptId = attemptId;
    }

    public BigInteger getTransactionId() {
        return txid;
    }

    public long getAttemptId() {
        return attemptId;
    }

    @Override
    public int hashCode() {
        return txid.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TransactionAttempt)) {
            return false;
        }
        TransactionAttempt other = (TransactionAttempt) o;
        return txid.equals(other.txid) && attemptId == other.attemptId;
    }

    @Override
    public String toString() {
        return "" + txid + ":" + attemptId;
    }
}
