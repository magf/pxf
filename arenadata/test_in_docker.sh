#!/usr/bin/env bash
# This script depends on hub.adsw.io/library/gpdb6_pxf_regress
set -exo

export GPHOME=/usr/local/greenplum-db-devel
export GP_PATH_FILE=greenplum_path.sh

if [[ -f bin_gpdb/bin_gpdb.tar.gz ]]; then
  count=$(tar -tvf bin_gpdb/bin_gpdb.tar.gz | grep greengage_path.sh | wc -l)
  if [ $count -ge 1 ]; then
    export GPHOME=/usr/local/greengage-db-devel
    export GP_PATH_FILE=greengage_path.sh
  fi
fi

# manually prepare gpadmin user; test_pxf.bash doesn't tweak gpadmin folder permissions and ssh keys
./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
# unpack gpdb and pxf; run gpdb cluster and pxf server
GPHOME=$GPHOME GP_PATH_FILE=$GP_PATH_FILE /tmp/build/pxf_src/concourse/scripts/test_pxf.bash
# tweak necessary folders to run regression tests later
chown gpadmin:gpadmin -R ${GPHOME}
chown gpadmin:gpadmin -R /tmp/build/pxf_src

# Display the diff if we fail
trap "cat /tmp/build/pxf_src/fdw/regression.diffs /tmp/build/pxf_src/external-table/regression.diffs" ERR

# test fdw and external-table
test_command='source '${GPHOME}'/'${GP_PATH_FILE}';
    source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh;
    cd /tmp/build/pxf_src/fdw &&
    make install &&
    make installcheck &&
    cd ../external-table/ &&
    make install &&
    make installcheck;'

su - gpadmin -c "$test_command"