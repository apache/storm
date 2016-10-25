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
