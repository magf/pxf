#!/bin/bash

set -eoux pipefail

# use a login shell for setting environment
bash --login -c "
	export PXF_HOME=${GPHOME}/pxf
	make -C '${PWD}/pxf_src/external-table' install
  make -C '${PWD}/pxf_src/cli' install
  make -C '${PWD}/pxf_src/server' install-server
"

bash --login -c "chown -R gpadmin:gpadmin ${GPHOME}"
bash --login -c "chown -R gpadmin:gpadmin ${PWD}/pxf_src"

# install pxf extension
su - gpadmin -c "
    source ${GPHOME}/greengage_path.sh;
    cd ${PWD}/pxf_src/fdw &&
    make install &&
    cd ../external-table/ &&
    make install;"
