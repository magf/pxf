#!/bin/bash

echo "=============================="
echo "Build PXF image for automation"
echo "=============================="
pushd ../..
docker build -t gpdb6_pxf_automation:it -f automation/arenadata/Dockerfile .
popd
