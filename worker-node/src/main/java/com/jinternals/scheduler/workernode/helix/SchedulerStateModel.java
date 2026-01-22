package com.jinternals.scheduler.workernode.helix;

import com.jinternals.scheduler.workernode.service.PartitionManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "SLAVE", "MASTER" })
public class SchedulerStateModel extends StateModel {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerStateModel.class);
    private final String partitionName;
    private final int partitionId;
    private final PartitionManager partitionManager;

    public SchedulerStateModel(String partitionName,
            PartitionManager partitionManager) {
        this.partitionName = partitionName;
        this.partitionManager = partitionManager;
        // partitionName format is usually ResourceName_PartitionId
        String[] parts = partitionName.split("_");
        this.partitionId = Integer.parseInt(parts[parts.length - 1]);
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
        logger.info("Transitioning from OFFLINE to SLAVE for partition: {}", partitionName);
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
        logger.info("Transitioning from SLAVE to MASTER for partition: {}", partitionName);
        logger.info("Starting scheduler for partition {}", partitionId);
        partitionManager.addPartition(partitionId);
    }

    @Transition(from = "MASTER", to = "SLAVE")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
        logger.info("Transitioning from MASTER to SLAVE for partition: {}", partitionName);
        logger.info("Stopping scheduler for partition {}", partitionId);
        partitionManager.removePartition(partitionId);
    }

    @Transition(from = "SLAVE", to = "OFFLINE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
        logger.info("Transitioning from SLAVE to OFFLINE for partition: {}", partitionName);
        partitionManager.removePartition(partitionId); // Ensure cleanup
    }
}
