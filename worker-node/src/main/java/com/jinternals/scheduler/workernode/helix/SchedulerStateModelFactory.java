package com.jinternals.scheduler.workernode.helix;

import com.jinternals.scheduler.workernode.service.PartitionManager;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class SchedulerStateModelFactory extends StateModelFactory<StateModel> {
    private final PartitionManager partitionManager;

    public SchedulerStateModelFactory(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
        return new SchedulerStateModel(partitionName, partitionManager);
    }
}
