package com.jinternals.scheduler.workernode.configuration;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.NotificationContext; // Correct signature
import org.springframework.context.annotation.Bean;
import java.util.UUID;
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

    @Value("${helix.instance.name:}")
    private String instanceName;

    private HelixManager helixManager;
    private HelixAdmin helixAdmin;

    @PostConstruct
    public void start() {
        if (instanceName == null || instanceName.isEmpty()) {
            instanceName = "controller-" + UUID.randomUUID().toString();
        }

        logger.info("Starting Helix Controller: cluster={}, zk={}, instance={}", clusterName, zkAddress, instanceName);

        try {
            helixManager = HelixManagerFactory.getZKHelixManager(
                    clusterName,
                    instanceName,
                    InstanceType.CONTROLLER,
                    zkAddress);

            helixManager.connect();

            // Register a listener to know if we are Leader or Standby
            helixManager.addControllerListener((ControllerChangeListener) notification -> {
                // Basic hook to monitor controller changes
            });

            helixAdmin = new ZKHelixAdmin(zkAddress);

            logger.info("Helix Controller connected successfully. Leader Status: {}", helixManager.isLeader());
        } catch (Exception e) {
            logger.error("Failed to connect Helix Controller", e);
            throw new RuntimeException(e);
        }
    }

    @Bean
    public HelixAdmin helixAdmin() {
        return helixAdmin;
    }

    /**
     * Operational method to toggle Maintenance Mode.
     * Can be exposed via JMX or REST Endpoint.
     */
    public void setMaintenanceMode(boolean enable, String reason) {
        if (helixAdmin != null) {
            logger.warn("Setting Cluster Maintenance Mode to: {} (Reason: {})", enable, reason);
            helixAdmin.enableMaintenanceMode(clusterName, enable, reason);
        }
    }

    @PreDestroy
    public void stop() {
        if (helixManager != null && helixManager.isConnected()) {
            logger.info("Disconnecting Helix Controller...");
            helixManager.disconnect();
        }
        if (helixAdmin != null) {
            helixAdmin.close();
        }
    }
}
