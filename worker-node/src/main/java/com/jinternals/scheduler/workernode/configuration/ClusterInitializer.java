package com.jinternals.scheduler.workernode.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("init")
@Slf4j
public class ClusterInitializer implements CommandLineRunner {

    @Value("${helix.cluster.name}")
    private String clusterName;

    @Value("${helix.zookeeper.address}")
    private String zkAddress;

    @Value("${scheduler.partitions:6}")
    private int numPartitions;

    @Override
    @SuppressWarnings("deprecation")
    public void run(String... args) throws Exception {
        log.info("Initializing Helix Cluster: {}", clusterName);
        ZKHelixAdmin admin = new ZKHelixAdmin(zkAddress);
        try {
            // Create Cluster
            if (!admin.getClusters().contains(clusterName)) {
                admin.addCluster(clusterName);
                log.info("Cluster created.");
            } else {
                log.info("Cluster already exists.");
            }

            // Ensure MasterSlave state model exists
            if (!admin.getStateModelDefs(clusterName).contains(BuiltInStateModelDefinitions.MasterSlave.name())) {
                admin.addStateModelDef(clusterName, BuiltInStateModelDefinitions.MasterSlave.name(),
                        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
                log.info("StateModelDefinition MasterSlave added.");
            }

            // Add Resource (e.g. "scheduler-resource") with 6 partitions and MasterSlave
            // model
            String resourceName = "scheduler-resource";
            if (!admin.getResourcesInCluster(clusterName).contains(resourceName)) {
                admin.addResource(clusterName, resourceName, numPartitions,
                        BuiltInStateModelDefinitions.MasterSlave.name(),
                        "AUTO_REBALANCE");
                log.info("Resource created: {}", resourceName);

                // Rebalance is needed for AUTO rebalance mode so the controller picks it up
                admin.rebalance(clusterName, resourceName, 2); // 2 replicas
                log.info("Resource rebalanced.");
            } else {
                log.info("Resource already exists.");
            }

            log.info("Initialization complete. Enabling allowParticipantAutoJoin.");

            ConfigAccessor configAccessor = new ConfigAccessor(zkAddress);
            ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
            if (clusterConfig == null) {
                clusterConfig = new ClusterConfig(clusterName);
            }
            // Use the generic property setting. The key is typically
            // "allowParticipantAutoJoin" (camelCase) in the record.
            // We set both to be safe against version differences.
            clusterConfig.getRecord().setSimpleField("allowParticipantAutoJoin", "true");
            clusterConfig.getRecord().setSimpleField("ALLOW_PARTICIPANT_AUTO_JOIN", "true");

            configAccessor.setClusterConfig(clusterName, clusterConfig);
            log.info("Auto join enabled successfully.");

            // Verify
            ClusterConfig verifyConfig = configAccessor.getClusterConfig(clusterName);
            log.info("Current Cluster Config: {}", verifyConfig.getRecord().getSimpleFields());

        } catch (Exception e) {
            log.error("Failed to enable auto join", e);
            e.printStackTrace();
        } finally {
            if (admin != null) {
                admin.close();
            }
            System.exit(0);
        }
    }
}
