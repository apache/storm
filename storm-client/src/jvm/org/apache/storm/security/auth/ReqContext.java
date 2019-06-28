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

package org.apache.storm.security.auth;

import java.net.InetAddress;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;

/**
 * Request context. Context request context includes info about:
 * <ol>
 * <li>remote address</li>
 * <li>remote subject and primary principal
 * <li>request ID</li>
 * </ol>
 */
public class ReqContext {
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    //each thread will have its own request context
    private static final ThreadLocal<ReqContext> ctxt =
        ThreadLocal.withInitial(() -> new ReqContext(AccessController.getContext()));
    private Subject subject;
    private InetAddress remoteAddr;
    private final int reqId;
    private Principal realPrincipal;

    //private constructor
    @VisibleForTesting
    public ReqContext(AccessControlContext aclCtxt) {
        subject = Subject.getSubject(aclCtxt);
        reqId = uniqueId.incrementAndGet();
    }

    //private constructor
    @VisibleForTesting
    public ReqContext(Subject sub) {
        subject = sub;
        reqId = uniqueId.incrementAndGet();
    }

    /**
     * Copy Constructor.
     */
    @VisibleForTesting
    public ReqContext(ReqContext other) {
        subject = other.subject;
        remoteAddr = other.remoteAddr;
        reqId = other.reqId;
        realPrincipal = other.realPrincipal;
    }

    /**
     * Get context.
     * @return a request context associated with current thread
     */
    public static ReqContext context() {
        return ctxt.get();
    }

    /**
     * Reset the context back to a default.  used for testing.
     */
    public static void reset() {
        ctxt.remove();
    }

    @Override
    public String toString() {
        return "ReqContext{"
                + "realPrincipal=" + ((realPrincipal != null) ? realPrincipal.getName() : "null")
                + ", reqId=" + reqId
                + ", remoteAddr=" + remoteAddr
                + ", authZPrincipal=" + ((principal() != null) ? principal().getName() : "null")
                + ", ThreadId=" + Thread.currentThread().toString()
                + '}';
    }

    /**
     * client address.
     */
    public void setRemoteAddress(InetAddress addr) {
        remoteAddr = addr;
    }

    public InetAddress remoteAddress() {
        return remoteAddr;
    }

    /**
     * Set remote subject explicitly.
     */
    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    /**
     * Retrieve client subject associated with this request context.
     */
    public Subject subject() {
        return subject;
    }

    /**
     * The primary principal associated current subject.
     */
    public Principal principal() {
        if (subject == null) {
            return null;
        }
        Set<Principal> princs = subject.getPrincipals();
        if (princs.size() == 0) {
            return null;
        }
        return (Principal) (princs.toArray()[0]);
    }

    public void setRealPrincipal(Principal realPrincipal) {
        this.realPrincipal = realPrincipal;
    }

    /**
     * The real principal associated with the subject.
     */
    public Principal realPrincipal() {
        return this.realPrincipal;
    }

    /**
     * Check whether context is impersonating.
     * @return true if this request is an impersonation request.
     */
    public boolean isImpersonating() {
        return this.realPrincipal != null && !this.realPrincipal.equals(this.principal());
    }

    /**
     * request ID of this request.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public int requestID() {
        return reqId;
    }
}
