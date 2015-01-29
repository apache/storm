package storm.kafka.failures;

import storm.kafka.DefaultFailureHandler;
import storm.kafka.PartitionManager;

import java.util.Map;

/**
 * Created by olgagorun on 1/29/15.
 */
public class MessageFailureHandlerDefaultFactory implements IMessageFailureHandlerFactory {

    private enum FactoryType { DEFAULT, CONFIGURED_RETRY; }

    private FactoryType _factoryType;

    public MessageFailureHandlerDefaultFactory(String factoryType) {
        _factoryType = FactoryType.valueOf(factoryType);
    }

    @Override
    public MassageFailureHandler getHandler(PartitionManager pm, Map conf) {
        MassageFailureHandler handler = null;
        switch (_factoryType) {
            case DEFAULT:
                handler = new DefaultFailureHandler(pm);
                break;
            default:
                throw new RuntimeException("Unsupported factory type: " + _factoryType);

        }
        return handler;
    }
}
