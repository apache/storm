package org.apache.storm.cassandra.trident.state;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;

public interface StateMapper<T> extends Serializable {

    Fields getStateFields();

    Values toValues(T value);

    T fromValues(List<Values> values);

}
