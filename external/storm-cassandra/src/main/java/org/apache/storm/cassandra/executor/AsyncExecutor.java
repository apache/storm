/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.executor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.topology.FailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to asynchronously executes cassandra statements.
 */
public class AsyncExecutor<T> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutor.class);

    protected CqlSession session;

    protected ExecutorService executorService;

    protected AsyncResultHandler<T> handler;

    private AtomicInteger pending = new AtomicInteger();

    /**
     * Creates a new {@link AsyncExecutor} instance.
     */
    protected AsyncExecutor(CqlSession session, AsyncResultHandler<T> handler) {
        this(session, newSingleThreadExecutor(), handler);
    }

    /**
     * Creates a new {@link AsyncExecutor} instance.
     *
     * @param session The cassandra session.
     * @param executorService The executor service responsible to execute handler.
     */
    private AsyncExecutor(CqlSession session, ExecutorService executorService, AsyncResultHandler<T> handler) {
        this.session = session;
        this.executorService = executorService;
        this.handler = handler;
    }

    protected static ExecutorService newSingleThreadExecutor() {
        return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("cassandra-async-handler-%d").build());
    }

    /**
     * Asynchronously executes all statements associated to the specified input.
     * The input will be passed to handler#onSuccess once all queries succeed or to handler#onFailure if any one of them fails.
     */
    public List<SettableFuture<T>> execAsync(List<Statement> statements, final T input) {

        List<SettableFuture<T>> settableFutures = new ArrayList<>(statements.size());

        for (Statement s : statements) {
            settableFutures.add(execAsync(s, input, AsyncResultHandler.NO_OP_HANDLER));
        }

        ListenableFuture<List<T>> allAsList = Futures.allAsList(settableFutures);
        Futures.addCallback(allAsList, new FutureCallback<List<T>>() {
            @Override
            public void onSuccess(List<T> inputs) {
                handler.success(input);
            }

            @Override
            public void onFailure(Throwable t) {
                handler.failure(t, input);
            }
        }, executorService);
        return settableFutures;
    }

    /**
     * Asynchronously executes the specified batch statement. Inputs will be passed to
     * the {@link #handler} once query succeed or failed.
     */
    public SettableFuture<T> execAsync(final Statement statement, final T inputs) {
        return execAsync(statement, inputs, handler);
    }

    /**
     * Asynchronously executes the specified batch statement. Inputs will be passed to
     * the {@link #handler} once query succeed or failed.
     */
    public SettableFuture<T> execAsync(final Statement statement, final T inputs, final AsyncResultHandler<T> handler) {
        final SettableFuture<T> settableFuture = SettableFuture.create();
        pending.incrementAndGet();
        CompletionStage<AsyncResultSet> future = session.executeAsync(statement);
        future
            .thenAccept(
                result -> {
                    pending.decrementAndGet();
                    settableFuture.set(inputs);
                    handler.success(inputs);
                })
            .exceptionally(
                t -> {
                    pending.decrementAndGet();
                    LOG.error("Failed to execute statement '{}' ", statement, t);
                    settableFuture.setException(t);
                    handler.failure(t, inputs);
                    return null;
                });
        //Futures.addCallback(future, new FutureCallback<ResultSet>() {
        //    public void release() {
        //        pending.decrementAndGet();
        //    }
        //
        //    @Override
        //    public void onSuccess(ResultSet result) {
        //        release();
        //        settableFuture.set(inputs);
        //        handler.success(inputs);
        //    }
        //
        //    @Override
        //    public void onFailure(Throwable t) {
        //        LOG.error(String.format("Failed to execute statement '%s' ", statement), t);
        //        release();
        //        settableFuture.setException(t);
        //        handler.failure(t, inputs);
        //    }
        //}, executorService);
        return settableFuture;
    }

    /**
     * Asynchronously executes the specified select statements. Results will be passed to the {@link AsyncResultSetHandler}
     * once each query has succeed or failed.
     */
    public SettableFuture<List<T>> execAsync(final List<Statement> statements, final List<T> inputs, Semaphore throttle,
                                             final AsyncResultSetHandler<T> handler) {

        final SettableFuture<List<T>> settableFuture = SettableFuture.create();
        if (inputs.size() == 0) {
            settableFuture.set(new ArrayList<T>());
            return settableFuture;
        }

        final AsyncContext<T> asyncContext = new AsyncContext<>(inputs, throttle, settableFuture);
        for (int i = 0; i < statements.size(); i++) {

            // Acquire a slot
            if (asyncContext.acquire()) {
                try {
                    pending.incrementAndGet();
                    final T input = inputs.get(i);
                    final Statement statement = statements.get(i);
                    CompletionStage<AsyncResultSet> future = session.executeAsync(statement);
                    future
                        .thenAccept(
                            result -> {
                                try {
                                    ResultSet r = (ResultSet) result;
                                    handler.success(input, result);
                                } catch (Throwable throwable) {
                                    asyncContext.exception(throwable);
                                } finally {
                                    pending.decrementAndGet();
                                    asyncContext.release();
                                }
                            })
                        .exceptionally(
                            t -> {
                                try {
                                    handler.failure(t, input);
                                } catch (Throwable throwable2) {
                                    asyncContext.exception(throwable2);
                                } finally {
                                    asyncContext.exception(t).release();
                                    pending.decrementAndGet();
                                    LOG.error("Failed to execute statement '{}' ", statement, t);
                                }
                                return null;
                            });
                    //    Futures.addCallback(future, new FutureCallback<ResultSet>() {
                    //        @Override
                    //        public void onSuccess(ResultSet result) {
                    //            try {
                    //                handler.success(input, result);
                    //            } catch (Throwable throwable) {
                    //                asyncContext.exception(throwable);
                    //            } finally {
                    //                pending.decrementAndGet();
                    //                asyncContext.release();
                    //            }
                    //        }
                    //
                    //        @Override
                    //        public void onFailure(Throwable throwable) {
                    //            try {
                    //                handler.failure(throwable, input);
                    //            } catch (Throwable throwable2) {
                    //                asyncContext.exception(throwable2);
                    //            } finally {
                    //                asyncContext
                    //                    .exception(throwable)
                    //                    .release();
                    //                pending.decrementAndGet();
                    //                LOG.error(String.format("Failed to execute statement '%s' ", statement), throwable);
                    //            }
                    //        }
                    //    }, executorService);
                } catch (Throwable throwable) {
                    asyncContext.exception(throwable).release();
                    pending.decrementAndGet();
                    break;
                }
            }
        }
        return settableFuture;
    }

    /**
     * Returns the number of currently executed tasks which are not yet completed.
     */
    public int getPendingTasksSize() {
        return this.pending.intValue();
    }

    public void shutdown() {
        if (!executorService.isShutdown()) {
            LOG.info("shutting down async handler executor");
            this.executorService.shutdownNow();
        }
    }

    private static class AsyncContext<T> {
        private final List<T> inputs;
        private final SettableFuture<List<T>> future;
        private final AtomicInteger latch;
        private final List<Throwable> exceptions;
        private final Semaphore throttle;

        AsyncContext(List<T> inputs, Semaphore throttle, SettableFuture<List<T>> settableFuture) {
            this.inputs = inputs;
            this.latch = new AtomicInteger(inputs.size());
            this.throttle = throttle;
            this.exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
            this.future = settableFuture;
        }

        public boolean acquire() {
            throttle.acquireUninterruptibly();
            // Don't start new requests if there is an exception
            if (exceptions.size() > 0) {
                latch.decrementAndGet();
                throttle.release();
                return false;
            }
            return true;
        }

        public AsyncContext release() {
            int remaining = latch.decrementAndGet();
            if (remaining == 0) {
                if (exceptions.size() == 0) {
                    future.set(inputs);
                } else {
                    future.setException(new MultiFailedException(exceptions));
                }

            }
            throttle.release();
            return this;
        }

        public AsyncContext exception(Throwable throwable) {
            this.exceptions.add(throwable);
            return this;
        }
    }

    public static class MultiFailedException extends FailedException {
        private final List<Throwable> exceptions;

        public MultiFailedException(List<Throwable> exceptions) {
            super(getMessage(exceptions), exceptions.get(0));
            this.exceptions = exceptions;
        }

        private static String getMessage(List<Throwable> exceptions) {
            int top5 = Math.min(exceptions.size(), 5);
            StringBuilder sb = new StringBuilder();
            sb.append("First ")
                    .append(top5)
                    .append(" exceptions: ")
                    .append(System.lineSeparator());
            for (int i = 0; i < top5; i++) {
                sb.append(exceptions.get(i).getMessage())
                        .append(System.lineSeparator());
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(getMessage())
                    .append(System.lineSeparator())
                    .append("Multiple exceptions encountered: ")
                    .append(System.lineSeparator());

            for (Throwable exception : exceptions) {
                sb.append(exception.toString())
                        .append(System.lineSeparator());
            }

            return super.toString();
        }

        public List<Throwable> getExceptions() {
            return exceptions;
        }
    }
}
