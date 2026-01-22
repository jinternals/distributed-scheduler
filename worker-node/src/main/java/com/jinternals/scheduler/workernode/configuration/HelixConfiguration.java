package com.jinternals.scheduler.workernode.configuration;

import com.jinternals.scheduler.workernode.helix.SchedulerStateModelFactory;
import com.jinternals.scheduler.workernode.service.PartitionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!init & !controller")
public class HelixConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HelixConfiguration.class);

    @Value("${helix.cluster.name}")
    private String clusterName;

    @Value("${helix.zookeeper.address}")
    private String zkAddress;

    @Value("${helix.instance.name}")
    private String instanceName;

    private HelixManager helixManager;

    private final PartitionManager partitionManager;

    public HelixConfiguration(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    @PostConstruct
    public void start() {
        logger.info("Starting Helix Participant: cluster={}, instance={}, zk={}", clusterName, instanceName, zkAddress);
        try {
            helixManager = HelixManagerFactory.getZKHelixManager(
                    clusterName,
                    instanceName,
                    InstanceType.PARTICIPANT,
                    zkAddress);

            helixManager.getStateMachineEngine().registerStateModelFactory(
                    BuiltInStateModelDefinitions.MasterSlave.name(),
                    new SchedulerStateModelFactory(partitionManager));

            helixManager.connect();
            logger.info("Helix Participant connected successfully.");
        } catch (Exception e) {
            logger.error("Failed to connect Helix Participant", e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void stop() {
        if (helixManager != null && helixManager.isConnected()) {
            logger.info("Disconnecting Helix Participant...");
            helixManager.disconnect();
        }
    }
}
