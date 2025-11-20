#!/bin/bash
# For local RUN script build_in_docker.sh using developer's Greengage image

set -eux

export GOPATH=/opt/go
export DEV_HOME=/home/gpadmin
export GPHOME=/usr/local/greengage-db-devel
export PXF_HOME=$GPHOME/pxf
export PXF_SRC=$DEV_HOME/pxf
export GGDB_IMAGE=ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
export PATH=$GPHOME/bin:$PXF_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go/bin:$GOPATH/bin
export JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8'
export DEBIAN_FRONTEND='noninteractive'

docker run --name pxf_build_in_docker --rm -it \
  --mount type=tmpfs,destination=/tmp,tmpfs-mode=1777,tmpfs-size=128m \
  --mount type=bind,source="$(pwd)",target="$PXF_SRC" \
  --workdir="$PXF_SRC" \
  --env GOPATH \
  --env DEV_HOME \
  --env GPHOME \
  --env PXF_HOME \
  --env PATH \
  --env JAVA_TOOL_OPTIONS \
  --env DEBIAN_FRONTEND \
  $GGDB_IMAGE bash ci/build_in_docker.sh
