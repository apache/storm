package org.apache.storm.sql.runtime;

import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;

public class SimpleSqlTridentConsumer implements ISqlTridentDataSource.SqlTridentConsumer {
    private final StateFactory stateFactory;
    private final StateUpdater stateUpdater;

    public SimpleSqlTridentConsumer(StateFactory stateFactory, StateUpdater stateUpdater) {
        this.stateFactory = stateFactory;
        this.stateUpdater = stateUpdater;
    }

    @Override
    public StateFactory getStateFactory() {
        return stateFactory;
    }

    @Override
    public StateUpdater getStateUpdater() {
        return stateUpdater;
    }
}
