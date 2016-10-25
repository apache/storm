package org.apache.storm.beam.translation.runtime;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.*;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class StormDoFnRunner extends SimpleDoFnRunner implements Serializable {
    public StormDoFnRunner(PipelineOptions options,
                           DoFn fn,
                           SideInputReader sideInputReader,
                           DoFnRunners.OutputManager outputManager,
                           TupleTag mainOutputTag, List sideOutputTags,
                           ExecutionContext.StepContext stepContext,
                           CounterSet.AddCounterMutator addCounterMutator,
                           WindowingStrategy windowingStrategy) {
        super(options, fn, sideInputReader, outputManager, mainOutputTag, sideOutputTags, stepContext,
                addCounterMutator, windowingStrategy);
    }
}
