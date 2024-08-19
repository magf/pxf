#!/bin/bash

echo "=============================="
echo "      Clean the project       "
echo "=============================="
#pushd ../../server
# TODO turn on after switching to java 17
#./gradlew clean
#popd

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
pushd ../..
docker build -t gpdb6_pxf_automation:it -f automation/arenadata/Dockerfile .
popd
