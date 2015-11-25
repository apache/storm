package backtype.storm.topology;

import java.io.Serializable;
import java.util.Map;

/*
 * This interface is used to notify the update of user configuration
 * for bolt and spout 
 */

public interface IDynamicComponent extends Serializable {
    public void update(Map conf);
}