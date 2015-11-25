package com.alibaba.jstorm.task.group;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;

public class MkLocalShuffer extends Shuffer {
    private static final Logger LOG = Logger.getLogger(MkLocalShuffer.class);

    private List<Integer> outTasks;
    private RandomRange randomrange;
    private Set<Integer> lastLocalNodeTasks;
    private IntervalCheck intervalCheck;
    private WorkerData workerData;
    private boolean isLocal;

    public MkLocalShuffer(List<Integer> workerTasks, List<Integer> allOutTasks,
                          WorkerData workerData) {
        super(workerData);
        List<Integer> localOutTasks = new ArrayList<Integer>();

        for (Integer outTask : allOutTasks) {
            if (workerTasks.contains(outTask)) {
                localOutTasks.add(outTask);
            }
        }
        this.workerData = workerData;
        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60);

        if (localOutTasks.size() != 0) {
            this.outTasks = localOutTasks;
            isLocal = true;
        } else {
            this.outTasks = new ArrayList<Integer>();
            this.outTasks.addAll(allOutTasks);
            refreshLocalNodeTasks();
            isLocal = false;
        }
        randomrange = new RandomRange(outTasks.size());
    }

    /**
     * Don't need to take care of multiple thread, One task one thread
     */
    private void refreshLocalNodeTasks() {
        Set<Integer> localNodeTasks = workerData.getLocalNodeTasks();

        if (localNodeTasks == null || localNodeTasks.equals(lastLocalNodeTasks) ) {
            return;
        }
        LOG.info("Old localNodeTasks:" + lastLocalNodeTasks + ", new:"
                + localNodeTasks);
        lastLocalNodeTasks = localNodeTasks;

        List<Integer> localNodeOutTasks = new ArrayList<Integer>();

        for (Integer outTask : outTasks) {
            if (localNodeTasks.contains(outTask)) {
                localNodeOutTasks.add(outTask);
            }
        }

        if (localNodeOutTasks.isEmpty() == false) {
            this.outTasks = localNodeOutTasks;
        }
        randomrange = new RandomRange(outTasks.size());
    }

    public List<Integer> grouper(List<Object> values) {
        if (!isLocal && intervalCheck.check()) {
            refreshLocalNodeTasks();
        }
        int index = getActiveTask(randomrange, outTasks);
        // If none active tasks were found, still send message to a task
        if (index == -1)
            index = randomrange.nextInt();

        return JStormUtils.mk_list(outTasks.get(index));
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
