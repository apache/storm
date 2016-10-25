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
package org.apache.storm.beam.translation.util;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;

/**
 * No-op StepContext implementation.
 */
public class DefaultStepContext implements ExecutionContext.StepContext {
    @Override
    public String getStepName() {
        return null;
    }

    @Override
    public String getTransformName() {
        return null;
    }

    @Override
    public void noteOutput(WindowedValue<?> windowedValue) {

    }

    @Override
    public void noteSideOutput(TupleTag<?> tupleTag, WindowedValue<?> windowedValue) {

    }

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(TupleTag<?> tupleTag, Iterable<WindowedValue<T>> iterable, Coder<Iterable<WindowedValue<T>>> coder, W w, Coder<W> coder1) throws IOException {

    }

    @Override
    public StateInternals<?> stateInternals() {
        return null;
    }

    @Override
    public TimerInternals timerInternals() {
        return null;
    }
}
