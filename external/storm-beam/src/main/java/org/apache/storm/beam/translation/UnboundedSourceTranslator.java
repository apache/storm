package org.apache.storm.beam.translation;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.storm.beam.StormPipelineOptions;
import org.apache.storm.beam.translation.runtime.UnboundedSourceSpout;

/**
 * Translates a Read.Unbounded into a Storm spout.
 * @param <T>
 */
public class UnboundedSourceTranslator<T> implements TransformTranslator<Read.Unbounded<T>> {
    public void translateNode(Read.Unbounded<T> transform, TranslationContext context) {
        UnboundedSource source = transform.getSource();
        StormPipelineOptions options = context.getOptions();
        UnboundedSourceSpout spout = new UnboundedSourceSpout(source, options);

        String name = context.getCurrentTransform().getFullName();
        context.addSpout(name, spout);
    }
}
