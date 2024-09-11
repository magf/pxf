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
docker build -f Dockerfile -t cloud-hub.adsw.io/library/pxf-hadoop:3.3.6 .
popd

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
pushd ../..
docker build -t gpdb6_pxf_automation:it --build-arg "GPDB_IMAGE=${GPDB_IMAGE:-hub.adsw.io/library/gpdb6_regress:adb-6.x-dev}" -f automation/arenadata/Dockerfile .
popd
