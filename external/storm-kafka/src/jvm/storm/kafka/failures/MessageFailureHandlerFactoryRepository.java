package storm.kafka.failures;

/**
 * Created by olgagorun on 1/29/15.
 */
public final class MessageFailureHandlerFactoryRepository {

    private static IMessageFailureHandlerFactory _factory;

    private MessageFailureHandlerFactoryRepository() {    }

    public static void setFactory(IMessageFailureHandlerFactory factory) {
        _factory = factory;
    }

    public static IMessageFailureHandlerFactory getFactory() {
        if (_factory == null) {
            _factory = new MessageFailureHandlerDefaultFactory("CONFIGURED_RETRY");
        }
        return _factory;
    }
}
