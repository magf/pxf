#!/bin/bash

echo "=============================="
echo "      Clean the project       "
echo "=============================="
pushd ../../server
./gradlew clean
popd

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
pushd ../..
docker build -t gpdb6_pxf_automation:it -f automation/arenadata/Dockerfile .
popd
