#!/bin/bash

# Define paths
NIFI_VERSION="2.0.0-SNAPSHOT"
NIFI_HOME_DIR="../../nifi-assembly/target/nifi-${NIFI_VERSION}-bin/nifi-${NIFI_VERSION}"
NIFI_LIB_DIR="${NIFI_HOME_DIR}/lib"
NIFI_BIN_DIR="${NIFI_HOME_DIR}/bin"
PROJECT_DIR="."

# Run Maven to build the project
cd "${PROJECT_DIR}" || exit
mvn clean install

# Move the NAR file
mv target/*.nar "${NIFI_LIB_DIR}"

# Stop NiFi
"${NIFI_BIN_DIR}/nifi.sh" stop

# Start NiFi
"${NIFI_BIN_DIR}/nifi.sh" start --illegal-access=warn



