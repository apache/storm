package org.apache.storm.beam;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by tgoetz on 7/28/16.
 */
public class RandomSentenceSource extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

    private final Coder<String> coder;

    public RandomSentenceSource(Coder<String> coder){
        this.coder = coder;
    }

    @Override
    public List<? extends UnboundedSource<String, CheckpointMark>> generateInitialSplits(int i, PipelineOptions pipelineOptions) throws Exception {
        return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions pipelineOptions, @Nullable CheckpointMark checkpointMark) throws IOException {
        return new RandomSentenceReader(this);
    }

    @Nullable
    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
        return null;
    }

    @Override
    public void validate() {

    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
        return this.coder;
    }



    public static class RandomSentenceReader extends UnboundedReader<String> {

        private String[] values = {"blah blah blah", "foo bar", "my dog has fleas"};
        private int index = 0;
        private final UnboundedSource<String, CheckpointMark> source;

        public RandomSentenceReader(UnboundedSource<String, CheckpointMark> source){
            this.source = source;
        }


        @Override
        public boolean start() throws IOException {
            index = 0;
            return true;
        }

        @Override
        public boolean advance() throws IOException {
            index++;
            if(index == values.length){
                index = 0;
            }
            return true;
        }

        @Override
        public Instant getWatermark() {
            return Instant.now();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return null;
        }

        @Override
        public UnboundedSource<String, ?> getCurrentSource() {
            return this.source;
        }

        @Override
        public String getCurrent() throws NoSuchElementException {
            return values[index];
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return Instant.now();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
