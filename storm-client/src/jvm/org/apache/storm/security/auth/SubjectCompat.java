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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import javax.security.auth.Subject;

/**
 * Compatibility shim for {@link Subject} methods removed in Java 24.
 *
 * <ul>
 *   <li>{@code Subject.getSubject(AccessControlContext)} &rarr; {@code Subject.current()} (Java 18+)</li>
 *   <li>{@code Subject.doAs(Subject, PrivilegedExceptionAction)} &rarr; {@code Subject.callAs(Subject, Callable)} (Java 18+)</li>
 * </ul>
 *
 * <p>All dispatch is resolved once at class-init via {@link MethodHandle}, so there is no per-call reflection overhead.
 */
public final class SubjectCompat {

    private static final MethodHandle CURRENT = lookupCurrent();
    private static final MethodHandle CALL_AS = lookupCallAs();
    private static final boolean USE_CALL_AS = probeCallAs();

    private SubjectCompat() {
    }

    /**
     * Return the current {@link Subject}, equivalent to {@code Subject.current()} on Java 18+
     * or {@code Subject.getSubject(AccessController.getContext())} on Java 17.
     */
    public static Subject currentSubject() {
        try {
            return (Subject) CURRENT.invoke();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Execute a {@link Callable} as the given {@link Subject}, equivalent to
     * {@code Subject.callAs(subject, callable)} on Java 18+ or
     * {@code Subject.doAs(subject, privilegedAction)} on Java 17.
     *
     * @param subject the subject to run as (may be {@code null})
     * @param callable the action to execute
     * @param <T> return type
     * @return the callable's result
     * @throws Exception if the callable throws
     */
    @SuppressWarnings("unchecked")
    public static <T> T doAs(Subject subject, Callable<T> callable) throws Exception {
        try {
            if (USE_CALL_AS) {
                return (T) CALL_AS.invoke(subject, callable);
            } else {
                // Java 17 path: wrap Callable in PrivilegedExceptionAction for Subject.doAs()
                return (T) CALL_AS.invoke(subject,
                    (java.security.PrivilegedExceptionAction<T>) callable::call);
            }
        } catch (CompletionException e) {
            // Subject.callAs wraps in CompletionException
            unwrapAndThrow(e.getCause());
            throw e; // unreachable
        } catch (java.security.PrivilegedActionException e) {
            // Subject.doAs wraps in PrivilegedActionException
            unwrapAndThrow(e.getCause());
            throw e; // unreachable
        } catch (Exception e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static void unwrapAndThrow(Throwable cause) throws Exception {
        if (cause instanceof Exception) {
            throw (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        throw new RuntimeException(cause);
    }

    // --- MethodHandle lookups resolved once at class init ---

    private static boolean probeCallAs() {
        try {
            MethodHandles.lookup().findStatic(Subject.class, "callAs",
                MethodType.methodType(Object.class, Subject.class, Callable.class));
            return true;
        } catch (NoSuchMethodException | IllegalAccessException e) {
            return false;
        }
    }

    private static MethodHandle lookupCallAs() {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            // Java 18+: Subject.callAs(Subject, Callable)
            return lookup.findStatic(Subject.class, "callAs",
                MethodType.methodType(Object.class, Subject.class, Callable.class));
        } catch (NoSuchMethodException e) {
            // Java 17: Subject.doAs(Subject, PrivilegedExceptionAction)
            try {
                return lookup.findStatic(Subject.class, "doAs",
                    MethodType.methodType(Object.class, Subject.class,
                        java.security.PrivilegedExceptionAction.class));
            } catch (NoSuchMethodException | IllegalAccessException ex) {
                throw new AssertionError(ex);
            }
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle lookupCurrent() {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            // Subject.getSubject(AccessControlContext) is deprecated for removal and replaced by
            // Subject.current().
            // Lookup first the new API, since for Java versions where both exist, the
            // new API delegates to the old API (for example Java 18, 19 and 20).
            // Otherwise (Java 17), lookup the old API.
            return lookup.findStatic(Subject.class, "current",
                    MethodType.methodType(Subject.class));
        } catch (NoSuchMethodException e) {
            final MethodHandle getContext = lookupGetContext();
            final MethodHandle getSubject = lookupGetSubject();
            return MethodHandles.filterReturnValue(getContext, getSubject);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle lookupGetSubject() {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            final Class<?> contextClazz =
                    ClassLoader.getSystemClassLoader()
                            .loadClass("java.security.AccessControlContext");
            return lookup.findStatic(Subject.class, "getSubject",
                    MethodType.methodType(Subject.class, contextClazz));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle lookupGetContext() {
        try {
            final Class<?> controllerClazz =
                    ClassLoader.getSystemClassLoader().loadClass("java.security.AccessController");
            final Class<?> contextClazz =
                    ClassLoader.getSystemClassLoader()
                            .loadClass("java.security.AccessControlContext");
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            return lookup.findStatic(controllerClazz, "getContext",
                    MethodType.methodType(contextClazz));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }
}
