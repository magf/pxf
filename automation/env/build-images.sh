#!/bin/bash

echo "=============================="
echo "      Clean the project       "
echo "=============================="
pushd ../../server
./gradlew clean
popd

# Uncomment this section if image is not available in the docker registry
echo "===================================="
echo "      Build Hadoop 3.3.6 image      "
echo "===================================="
pushd hadoop
docker build -f Dockerfile -t pxf-hadoop:3.3.6 .
popd

#echo "===================================="
#echo "      Build Vault image      "
#echo "===================================="
docker build -f ./vault/Dockerfile -t pxf-vault-test:it .

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
# GGDB_IMAGE is from https://github.com/GreengageDB/greengage/tree/main/ci
pushd ../..
docker build -t ggdb6_pxf_automation:it --build-arg "GGDB_IMAGE=${GGDB_IMAGE:-ggdb6_u22:latest}" -f automation/env/Dockerfile .
popd
