package org.apache.storm.beam;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

public class StormRegistrar {
    private StormRegistrar(){}

    @AutoService(PipelineRunnerRegistrar.class)
    public static class Runner implements PipelineRunnerRegistrar {
        @Override
        public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
            return ImmutableList.<Class<? extends PipelineRunner<?>>>of(
                    StormRunner.class);
        }
    }

    @AutoService(PipelineOptionsRegistrar.class)
    public static class Options implements PipelineOptionsRegistrar {
        @Override
        public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
            return ImmutableList.<Class<? extends PipelineOptions>>of(
                    StormPipelineOptions.class);
        }
    }

}
