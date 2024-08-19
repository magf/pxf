#!/bin/bash

echo "=============================="
echo "      Clean the project       "
echo "=============================="
pushd ../../server
./gradlew clean
popd

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
docker build -t gpdb6_pxf_automation:it -f automation/arenadata/Dockerfile .
popd
