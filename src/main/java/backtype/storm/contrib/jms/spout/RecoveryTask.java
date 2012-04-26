package backtype.storm.contrib.jms.spout;

import java.util.TimerTask;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(RecoveryTask.class);
    private JmsSpout spout;

    public RecoveryTask(JmsSpout spout) {
        this.spout = spout;
    }

    public void run() {
        synchronized (spout.recoveryMutex) {
            if (spout.hasFailures()) {
                try {
                    LOG.info("Recovering from a message failure.");
                    spout.getSession().recover();
                    spout.recovered();
                } catch (JMSException e) {
                    LOG.warn("Could not recover jms session.", e);
                }
            }
        }
    }

}
