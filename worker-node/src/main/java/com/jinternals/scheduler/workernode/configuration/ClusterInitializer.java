package com.jinternals.scheduler.workernode.configuration;

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
public class ClusterInitializer implements CommandLineRunner {

    @Value("${helix.cluster.name}")
    private String clusterName;

    @Value("${helix.zookeeper.address}")
    private String zkAddress;

    @Override
    @SuppressWarnings("deprecation")
    public void run(String... args) throws Exception {
        System.out.println("Initializing Helix Cluster: " + clusterName);
        ZKHelixAdmin admin = new ZKHelixAdmin(zkAddress);
        try {
            // Create Cluster
            if (!admin.getClusters().contains(clusterName)) {
                admin.addCluster(clusterName);
                System.out.println("Cluster created.");
            } else {
                System.out.println("Cluster already exists.");
            }

            // Add Resource (e.g. "scheduler-resource") with 6 partitions and MasterSlave
            // model
            String resourceName = "scheduler-resource";
            if (!admin.getResourcesInCluster(clusterName).contains(resourceName)) {
                admin.addResource(clusterName, resourceName, 6, BuiltInStateModelDefinitions.MasterSlave.name(),
                        "AUTO_REBALANCE");
                System.out.println("Resource created: " + resourceName);

                // Rebalance is needed for AUTO rebalance mode so the controller picks it up
                admin.rebalance(clusterName, resourceName, 2); // 2 replicas
                System.out.println("Resource rebalanced.");
            } else {
                System.out.println("Resource already exists.");
            }

            System.out.println("Initialization complete. Enabling allowParticipantAutoJoin.");

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
            System.out.println("Auto join enabled successfully.");

            // Verify
            ClusterConfig verifyConfig = configAccessor.getClusterConfig(clusterName);
            System.out.println("Current Cluster Config: " + verifyConfig.getRecord().getSimpleFields());

        } catch (Exception e) {
            System.err.println("Failed to enable auto join: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (admin != null) {
                admin.close();
            }
            System.exit(0);
        }
    }
}
