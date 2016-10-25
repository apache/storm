package org.apache.storm.beam.translation;

import org.apache.beam.sdk.transforms.PTransform;

/**
 * Interface for classes capable of tranforming Beam PTransforms into Storm primitives.
 */
public interface TransformTranslator <Type extends PTransform>{

        void translateNode(Type transform, TranslationContext context);
}
