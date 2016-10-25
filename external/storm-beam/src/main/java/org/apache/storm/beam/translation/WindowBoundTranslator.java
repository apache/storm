package org.apache.storm.beam.translation;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;
import org.apache.storm.beam.translation.runtime.WindowBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a Window.Bound node into a Storm WindowedBolt
 * @param <T>
 */
public class WindowBoundTranslator<T> implements TransformTranslator<Window.Bound<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(WindowBoundTranslator.class);

    @Override
    public void translateNode(Window.Bound<T> transform, TranslationContext context) {
        if(transform.getWindowFn() instanceof FixedWindows){
            Duration size = ((FixedWindows) transform.getWindowFn()).getSize();

            WindowBolt bolt = new WindowBolt();
            bolt.withTumblingWindow(WindowBolt.Duration.seconds((int)size.getStandardSeconds()));

            PValue from = (PValue)context.getCurrentTransform().getInput();
            LOG.info(baseName(from.getName()));

            PValue to = (PValue)context.getCurrentTransform().getOutput();
            LOG.info(baseName(to.getName()));

            TranslationContext.Stream stream = new TranslationContext.Stream(baseName(from.getName()), baseName(to.getName()), new TranslationContext.Grouping(TranslationContext.Grouping.Type.SHUFFLE));

            context.addStream(stream);
            context.addBolt(baseName(to.getName()), bolt);

        } else {
            throw new UnsupportedOperationException("Currently only fixed windows are supported.");
        }
    }


    private static String baseName(String str){
        return str.substring(0, str.lastIndexOf("."));
    }
}
