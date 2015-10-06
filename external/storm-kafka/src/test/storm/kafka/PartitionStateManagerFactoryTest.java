package storm.kafka;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by 155715 on 10/5/15.
 */
public class PartitionStateManagerFactoryTest {

    PartitionStateManagerFactory factory;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testCustomStateStore() throws Exception {
        Map stormConfig = new HashMap();
        SpoutConfig spoutConfig = new SpoutConfig(null, null, null);
        spoutConfig.stateStore = TestStateStore.class.getName();

        factory = new PartitionStateManagerFactory(stormConfig, spoutConfig);

        PartitionStateManager partitionStateManager = factory.getInstance(null);

        assertEquals(TestStateStore.MAGIC_STATE, partitionStateManager.getState());
    }

    public static class TestStateStore implements StateStore {

        public static final Map<Object, Object> MAGIC_STATE = ImmutableMap.of(new Object(), new Object());

        public TestStateStore(Map stormConf, SpoutConfig spoutConfig) {}

        @Override
        public Map<Object, Object> readState(Partition p) {
            return MAGIC_STATE;
        }

        @Override
        public void writeState(Partition p, Map<Object, Object> state) {

        }

        @Override
        public void close() throws IOException {

        }
    }
}