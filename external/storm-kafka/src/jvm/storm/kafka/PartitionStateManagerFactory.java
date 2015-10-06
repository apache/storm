package storm.kafka;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static storm.kafka.SpoutConfig.STATE_STORE_KAFKA;
import static storm.kafka.SpoutConfig.STATE_STORE_ZOOKEEPER;

public class PartitionStateManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateStore.class);

    private StateStore _stateStore;

    private StateStore createZkStateStore(Map conf, SpoutConfig spoutConfig) {
        return new ZkStateStore(conf, spoutConfig);
    }

    private StateStore createKafkaStateStore(Map conf, SpoutConfig spoutConfig) {
        return new KafkaStateStore(conf, spoutConfig);
    }

    private StateStore createCustomStateStore(Map conf, SpoutConfig spoutConfig, String customStateStoreClazzName) {

        Class<?> customStateStoreClazz;
        try {
            customStateStoreClazz = Class.forName(customStateStoreClazzName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Invalid value defined for _spoutConfig.stateStore: %s. "
                            + "Valid values are %s, %s or name of the custom state store class. Default to %s",
                    spoutConfig.stateStore, STATE_STORE_ZOOKEEPER, STATE_STORE_KAFKA, STATE_STORE_ZOOKEEPER));
        }

        if (!StateStore.class.isAssignableFrom(customStateStoreClazz)) {
            throw new RuntimeException(String.format("Invalid custom state store class: %s. "
                                + "Must implement interface " + StateStore.class.getCanonicalName(),
                        spoutConfig.stateStore));
        }

        Constructor<?> constructor;
        try {
            constructor = customStateStoreClazz.getConstructor(Map.class, SpoutConfig.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Invalid custom state store class: %s. "
                                + "Must define a constructor with two parameters: Map conf, SpoutConfig spoutConfig",
                        spoutConfig.stateStore));
        }

        Object customStateStoreObj;
        try {
            customStateStoreObj= constructor.newInstance(conf, spoutConfig);
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format("Failed to instantiate custom state store class: %s due to InstantiationException.",
                        spoutConfig.stateStore), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("Failed to instantiate custom state store class: %s due to IllegalAccessException.",
                        spoutConfig.stateStore), e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(String.format("Failed to instantiate custom state store class: %s due to InvocationTargetException.",
                        spoutConfig.stateStore), e);
        }

        assert (customStateStoreObj instanceof StateStore);
        return (StateStore)customStateStoreObj;
    }

    public PartitionStateManagerFactory(Map stormConf, SpoutConfig spoutConfig) {

        // default to original storm storage format
        if (Strings.isNullOrEmpty(spoutConfig.stateStore) || STATE_STORE_ZOOKEEPER.equals(spoutConfig.stateStore)) {
            _stateStore = createZkStateStore(stormConf, spoutConfig);
            LOG.info("Created Zookeeper backed state store.");

        } else if (STATE_STORE_KAFKA.equals(spoutConfig.stateStore)) {
            _stateStore = createKafkaStateStore(stormConf, spoutConfig);
            LOG.info("Created Kafka backed state store.");

        } else {
            _stateStore = createCustomStateStore(stormConf, spoutConfig, spoutConfig.stateStore);
            LOG.info("Created custom state store implemented by {}.", spoutConfig.stateStore);
        }
    }

    public PartitionStateManager getInstance(Partition partition) {
        return new PartitionStateManager(partition, _stateStore);
    }

    public void close() {
        if (_stateStore != null) {
            IOUtils.closeQuietly(_stateStore);
        }
    }
}
