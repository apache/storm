package storm.kafka.failures;

import storm.kafka.*;

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
    public FailedMsgRetryManager getHandler(PartitionManager pm, Map conf) {
        FailedMsgRetryManager handler = null;
//        switch (_factoryType) {
//            case DEFAULT:
//                handler = new DefaultFailureHandler(pm);
//                break;
//            case CONFIGURED_RETRY:
//                handler = new ConfigurableRetriesFailureHandler(pm, getRetriesConfiguration(pm));
//                break;
//            default:
//                throw new RuntimeException("Unsupported factory type: " + _factoryType);
//
//        }
        handler = new ExponentialBackoffMsgRetryManager(1, 1, 1, 1);
        return handler;
    }

    private int getRetriesConfiguration(PartitionManager pm) {
        int retriesNumber = 3;
        String key = "kafka.spout." + pm.getSpoutConfig().id + ".retries";
        Map stormConf = pm.getStormConfig();
        if (stormConf.containsKey(key)) {
            //retriesNumber = Integer.parseInt((String)stormConf.get(key));
            retriesNumber = ((Long)stormConf.get(key)).intValue();
        }
        else if (stormConf.containsKey("kafka.spout.retries")) {
            retriesNumber = ((Long)stormConf.get("kafka.spout.retries")).intValue();
        }

        return retriesNumber;
    }
}
