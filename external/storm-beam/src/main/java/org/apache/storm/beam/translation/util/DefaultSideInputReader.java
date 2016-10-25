package org.apache.storm.beam.translation.util;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * No-op SideInputReader implementation.
 */
public class DefaultSideInputReader implements SideInputReader, Serializable {
    @Nullable
    @Override
    public <T> T get(PCollectionView<T> pCollectionView, BoundedWindow boundedWindow) {
        return null;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> pCollectionView) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }
}
