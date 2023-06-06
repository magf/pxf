#!/usr/bin/env bash
# This script depends on hub.adsw.io/library/gpdb6_pxf_regress
set -exo pipefail

# manually prepare gpadmin user; test_pxf.bash doesn't tweak gpadmin folder permissions and ssh keys
./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
# unpack gpdb and pxf; run gpdb cluster and pxf server
/tmp/build/pxf_src/concourse/scripts/test_pxf.bash
# tweak necessary folders to run regression tests later
chown gpadmin:gpadmin -R /usr/local/greenplum-db-devel
chown gpadmin:gpadmin -R /tmp/build/pxf_src

# test fdw and external-table
su - gpadmin -c "
    source '/usr/local/greenplum-db-devel/greenplum_path.sh';
    source '/home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh';
    cd /tmp/build/pxf_src/fdw &&
    make install &&
    make installcheck &&
    cd ../external-table/ &&
    make install &&
    make installcheck;
"
