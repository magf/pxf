#!/bin/bash

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
pushd ../..
docker build -t gpdb6_pxf_automation:it -f automation/arenadata/Dockerfile .
popd

echo "================================="
echo "Build Hadoop image for automation"
echo "================================="
pushd ./hadoop
docker build -f Dockerfile -t pxf-hadoop:3.1.3 .
popd
