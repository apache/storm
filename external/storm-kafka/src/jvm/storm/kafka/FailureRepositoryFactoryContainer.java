package storm.kafka;

/**
 * Created by olgagorun on 6/29/15.
 */
public class FailureRepositoryFactoryContainer {

    private static IFailureRepositoryFactory factory = null;

    public static IFailureRepositoryFactory get() {
        return factory;
    }

    public static void set(IFailureRepositoryFactory repositoryFactory) {
        factory = repositoryFactory;
    }
}
