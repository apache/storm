package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;

/**
 * Enno Shioji
 */
public class MessageBatcher {
    private final int mesageBatchSize;
    private MessageBatch currentBatch;

    public MessageBatcher(int mesageBatchSize){
        this.mesageBatchSize = mesageBatchSize;
        this.currentBatch = new MessageBatch(mesageBatchSize);
    }

    public synchronized MessageBatch add(TaskMessage msg){
        currentBatch.add(msg);
        if(currentBatch.isFull()){
            MessageBatch ret = currentBatch;
            currentBatch = new MessageBatch(mesageBatchSize);
            return ret;
        } else {
            return null;
        }
    }

    public synchronized boolean isEmpty() {
        return currentBatch.isEmpty();
    }

    public synchronized MessageBatch drain() {
        MessageBatch ret = currentBatch;
        currentBatch = new MessageBatch(mesageBatchSize);
        return ret;
    }
}
