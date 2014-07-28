package storm.trident.state;

import java.util.Map;

public class JSONSerializerFactory implements SerializerFactory {

    @Override
    public Serializer makeSerializer(Map conf, StateType stateType) {
        if (stateType == StateType.NON_TRANSACTIONAL) {
            return new JSONNonTransactionalSerializer();
        }
        
        if (stateType == StateType.TRANSACTIONAL) {
            return new JSONTransactionalSerializer();
        }
        
        return new JSONOpaqueSerializer();
    }
}
