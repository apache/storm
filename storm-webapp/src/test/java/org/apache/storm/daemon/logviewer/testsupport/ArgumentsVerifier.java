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

package org.apache.storm.daemon.logviewer.testsupport;

import static org.junit.Assert.assertEquals;

import java.util.function.Consumer;
import org.mockito.ArgumentCaptor;

/**
 * Help verifying captured argument in Mockito.
 */
public class ArgumentsVerifier {
    /**
     * Asserting that method is called with expected first argument.
     *
     * @param verifyConsumer Consumer implementation that takes ArgumentCaptor and call 'Mockito.verify'
     * @param argClazz Class type for argument
     * @param expectedArg expected argument
     */
    public static <T> void verifyFirstCallArgsForSingleArgMethod(Consumer<ArgumentCaptor<T>> verifyConsumer,
                                                                 Class<T> argClazz, T expectedArg) {
        ArgumentCaptor<T> captor = ArgumentCaptor.forClass(argClazz);
        verifyConsumer.accept(captor);
        assertEquals(expectedArg, captor.getAllValues().get(0));
    }
}
