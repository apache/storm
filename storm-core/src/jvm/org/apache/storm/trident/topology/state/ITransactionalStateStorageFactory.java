package org.apache.storm.trident.topology.state;

import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface ITransactionalStateStorageFactory {
    ITransactionalStateStorage mkTransactionalState(Map conf, String id, String subroot);
}