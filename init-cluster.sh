#!/bin/bash

# Configuration
ZK_ADDRESS="localhost:2181"
CLUSTER_NAME="scheduler-cluster"
RESOURCE_NAME="events-topic"
NUM_PARTITIONS=6
REPLICAS=2

# This script assumes helix-admin.sh is available or we can use docker to run it.
# For simplicity in this dev environment, I'll attempt to use a docker container to run the java command
# or assume the user has helix tools. 
# Better yet, I'll provide a Java utility class in the project that can be run to init the cluster.
# But here is a script using the container if we had the tools.

# Since we don't have the tools installed on the host easily, I will create a Java class `ClusterInitializer` 
# in the worker-node that can be run with a special profile or main argument.

echo "Please run the Worker Node with profile 'init' to initialize the cluster."
