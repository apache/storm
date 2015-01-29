package storm.kafka.failures;

import storm.kafka.ConfigurableRetriesFailureHandler;
import storm.kafka.DefaultFailureHandler;
import storm.kafka.PartitionManager;

import java.util.Map;

/**
 * Created by olgagorun on 1/29/15.
 */
public class MessageFailureHandlerDefaultFactory implements IMessageFailureHandlerFactory {

    private enum FactoryType { DEFAULT, CONFIGURED_RETRY }

    private FactoryType _factoryType;

    public MessageFailureHandlerDefaultFactory(String factoryType) {
        _factoryType = FactoryType.valueOf(factoryType);
    }

    @Override
    public IMassageFailureHandler getHandler(PartitionManager pm, Map conf) {
        IMassageFailureHandler handler = null;
        switch (_factoryType) {
            case DEFAULT:
                handler = new DefaultFailureHandler(pm);
                break;
            case CONFIGURED_RETRY:
                handler = new ConfigurableRetriesFailureHandler(pm, getRetriesConfiguration(pm));
                break;
            default:
                throw new RuntimeException("Unsupported factory type: " + _factoryType);

        }
        return handler;
    }

    private int getRetriesConfiguration(PartitionManager pm) {
        int retriesNumber = 3;
        String key = "kafka.spout." + pm.getSpoutConfig().id + ".retries";
        Map stormConf = pm.getStormConfig();
        if (stormConf.containsKey(key)) {
            retriesNumber = (Integer)stormConf.get(key);
        }
        else if (stormConf.containsKey("kafka.spout.retries")) {
            retriesNumber = (Integer)stormConf.get("kafka.spout.retries");
        }

        return retriesNumber;
    }
}
