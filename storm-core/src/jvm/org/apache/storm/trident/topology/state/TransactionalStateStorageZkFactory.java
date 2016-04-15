package org.apache.storm.trident.topology.state;

import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class TransactionalStateStorageZkFactory implements ITransactionalStateStorageFactory {
    @Override
    public ITransactionalStateStorage mkTransactionalState(Map conf, String id, String subroot) {
        return new TransactionalStateZkStorage(conf, id, subroot);
    }
}

