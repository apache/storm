package storm.trident.state;

import java.util.Map;

public interface SerializerFactory<T> {

    Serializer<T> makeSerializer( Map conf, StateType stateType );
}
