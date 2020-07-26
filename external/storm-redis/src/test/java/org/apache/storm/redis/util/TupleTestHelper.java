package org.apache.storm.redis.util;

import org.apache.storm.redis.util.outputcollector.EmittedTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 *
 */
public class TupleTestHelper {

    public static void verifyAnchors(final EmittedTuple emittedTuple, final Tuple expectedAnchor) {
        Objects.requireNonNull(emittedTuple);
        Objects.requireNonNull(expectedAnchor);
        assertEquals(1, emittedTuple.getAnchors().size(), "Should have a single anchor");

        final Tuple anchor = emittedTuple.getAnchors().get(0);
        assertNotNull(anchor, "Should be non-null");
        assertEquals(expectedAnchor, anchor);
    }

    public static void verifyEmittedTuple(final EmittedTuple emittedTuple, final List<Object> expectedValues) {
        Objects.requireNonNull(emittedTuple);
        Objects.requireNonNull(expectedValues);

        assertEquals(expectedValues.size(), emittedTuple.getTuple().size());
        assertEquals(expectedValues, emittedTuple.getTuple());
    }
}
