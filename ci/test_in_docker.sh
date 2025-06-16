#!/usr/bin/env bash
# This script depends on ggdb6_pxf_regress
set -exo pipefail

# Set hostname to make certificate valid and PXF server accessible
hostname mdw
echo "127.0.0.1    mdw" >> /etc/hosts

# manually prepare gpadmin user; test_pxf.bash doesn't tweak gpadmin folder permissions and ssh keys
./gpdb_src/concourse/scripts/setup_gpadmin_user.bash

# Pass through PXF environment variables to gpadmin user
if [[ "$PXF_PROTOCOL" = "https" ]]; then
    echo "--------------------------------------"
    echo "Init SSL env variables for PXF service"
    echo "--------------------------------------"
    env | grep -E 'PXF_SSL|PXF_HOST|PXF_PROTOCOL' | sed 's/^/export /' >> /home/gpadmin/.bash_profile
    env | grep -E 'PXF_SSL|PXF_HOST|PXF_PROTOCOL' | sed 's/^/export /' >> /home/gpadmin/.bashrc
fi

# unpack gpdb and pxf; run gpdb cluster and pxf server
/tmp/build/pxf_src/concourse/scripts/test_pxf.bash

# Enable basePath in the PXF server to allow writable tables SSL test
if [[ "$PXF_PROTOCOL" = "https" ]]; then
  sed -i \
    -e 's|</configuration>|<property><name>pxf.fs.basePath</name><value>/tmp/pxf/</value></property></configuration>|g' \
    ${PXF_HOME}/servers/default/pxf-site.xml
fi

# tweak necessary folders to run regression tests later
chown gpadmin:gpadmin -R /usr/local/greengage-db-devel
chown gpadmin:gpadmin -R /tmp/build/pxf_src

# Display the diff if we fail
trap "cat /tmp/build/pxf_src/fdw/regression.diffs /tmp/build/pxf_src/external-table/regression.diffs" ERR

# test fdw and external-table
su - gpadmin -c "
    source '/usr/local/greengage-db-devel/greengage_path.sh';
    source '/home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh';
    cd /tmp/build/pxf_src/fdw &&
    make install &&
    make installcheck &&
    cd ../external-table/ &&
    make install &&
    make installcheck;
"
