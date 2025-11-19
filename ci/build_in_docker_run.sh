#!/bin/bash
# Fot local RUN script build_in_docker.sh using developer's Greengage image

set -eux

export GOPATH=/opt/go
export DEV_HOME=/home/gpadmin
export GPHOME=/usr/local/greengage-db-devel
export PXF_HOME=$GPHOME/pxf
export GGDB_IMAGE=ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
export PATH=$GPHOME/bin:$PXF_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go/bin:$GOPATH/bin
export JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8'
export DEBIAN_FRONTEND='noninteractive'

docker run --name pxf_build_in_docker --rm -it \
  --workdir="$PXF_SRC" \
  --volume ./:"$PXF_SRC" \
  --env=GOPATH,DEV_HOME,GPHOME,PXF_HOME,PATH,JAVA_TOOL_OPTIONS,DEBIAN_FRONTEND \
  $GGDB_IMAGE bash -c "ci/build_in_docker.sh"
