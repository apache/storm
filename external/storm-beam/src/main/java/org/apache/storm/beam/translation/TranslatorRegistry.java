package org.apache.storm.beam.translation;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;

import java.util.HashMap;
import java.util.Map;

/**
 * Lookup table mapping PTransform types to associated TransformTranslator implementations.
 */
public class TranslatorRegistry {
    private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = new HashMap();

    static {
        TRANSLATORS.put(Read.Unbounded.class, new UnboundedSourceTranslator());
        TRANSLATORS.put(Window.Bound.class, new WindowBoundTranslator<>());
        TRANSLATORS.put(ParDo.Bound.class, new ParDoBoundTranslator<>());
        TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslator<>());
    }

    static TransformTranslator<?> getTranslator(
            PTransform<?, ?> transform) {
        return TRANSLATORS.get(transform.getClass());
    }
}
