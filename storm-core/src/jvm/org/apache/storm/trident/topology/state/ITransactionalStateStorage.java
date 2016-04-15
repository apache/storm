package org.apache.storm.trident.topology.state;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface ITransactionalStateStorage {

     void setData(String path, Object obj);
     void delete(String path);
     List<String> list(String path);
     void mkdir(String path);
     Object getData(String path);
     void close();
}