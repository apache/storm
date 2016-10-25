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
package org.apache.storm.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * A minimal word count pipeline using the Beam API, running on top of Storm
 *
 * When the Storm Runner is reasonably complete, running this pipline in Storm
 * should yield that same output as running it on the Beam DirectRunner
 *
 */
public class StormWordCount {

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Aggregator<Long, Long> emptyLines =
                createAggregator("emptyLines", new Sum.SumLongFn());

        @Override
        public void processElement(ProcessContext c) {
            if (c.element().trim().isEmpty()) {
                emptyLines.addValue(1L);
            }

            // Split the line into words.
            String[] words = c.element().split("[^a-zA-Z']+");

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    System.out.println(word);
                    c.output(word);
                }
            }
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            String retval = input.getKey() + ": " + input.getValue();
            System.out.println(retval);
            return retval;
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     */
    public static class CountWords extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts =
                    words.apply(Count.<String>perElement());

            return wordCounts;
        }
    }

    /**
     * Options supported by {@link StormWordCount}.
     * <p>
     * <p>Inherits standard configuration options.
     */
    public interface WordCountOptions extends PipelineOptions {

    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountOptions.class);
        Pipeline p = Pipeline.create(options);
        p.apply("Spout", Read.from(new RandomSentenceSource(StringUtf8Coder.of())))
                .apply("Window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply("ExtractWords", ParDo.of(new ExtractWordsFn()))
                .apply(new CountWords());

        p.run();

    }
}
