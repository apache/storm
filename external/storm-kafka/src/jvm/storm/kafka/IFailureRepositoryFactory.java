package storm.kafka;

/**
 * Created by olgagorun on 6/29/15.
 */
public interface IFailureRepositoryFactory {
    public IFailureRepository getRepository();
}
