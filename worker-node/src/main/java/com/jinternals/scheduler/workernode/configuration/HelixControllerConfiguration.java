package com.jinternals.scheduler.workernode.configuration;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("controller")
public class HelixControllerConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HelixControllerConfiguration.class);

    @Value("${helix.cluster.name}")
    private String clusterName;

    @Value("${helix.zookeeper.address}")
    private String zkAddress;

    @Value("${helix.instance.name}")
    private String instanceName; // For controller, name matters less but good to have unique

    private HelixManager helixManager;

    @PostConstruct
    public void start() {
        logger.info("Starting Helix Controller: cluster={}, zk={}", clusterName, zkAddress);
        try {
            helixManager = HelixManagerFactory.getZKHelixManager(
                    clusterName,
                    "controller-" + instanceName,
                    InstanceType.CONTROLLER,
                    zkAddress);

            helixManager.connect();
            logger.info("Helix Controller connected successfully.");
        } catch (Exception e) {
            logger.error("Failed to connect Helix Controller", e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void stop() {
        if (helixManager != null && helixManager.isConnected()) {
            logger.info("Disconnecting Helix Controller...");
            helixManager.disconnect();
        }
    }
}
