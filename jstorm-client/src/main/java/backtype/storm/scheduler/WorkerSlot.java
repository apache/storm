package backtype.storm.scheduler;

public class WorkerSlot implements Comparable<WorkerSlot>{
    String nodeId;
    int port;
    
    public WorkerSlot(String nodeId, Number port) {
        this.nodeId = nodeId;
        this.port = port.intValue();
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode() + 13 * ((Integer) port).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        WorkerSlot other = (WorkerSlot) o;
        return this.port == other.port && this.nodeId.equals(other.nodeId);
    }    
    
    @Override
    public String toString() {
    	return this.nodeId + ":" + this.port;
    }

    @Override
    public int compareTo(WorkerSlot o) {
        String otherNode = o.getNodeId();
        if (nodeId == null) {
            if (otherNode != null) {
                return -1;
            }else {
                return port - o.getPort();
            }
        }else {
            int ret = nodeId.compareTo(otherNode);
            if (ret == 0) {
                return port - o.getPort();
            }else {
                return ret;
            }
        }
    }
}
