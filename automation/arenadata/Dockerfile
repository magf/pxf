FROM hub.adsw.io/library/gpdb6_regress:latest
RUN sudo yum install -y epel-release  maven go python-pip

# install go, ginkgo and keep env variables which may be used as a part of base image
ENV GOPATH=$HOME/go
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
RUN go install github.com/onsi/ginkgo/ginkgo@latest

# Install gpdb source files
WORKDIR /home/gpadmin/
RUN mkdir -p /data1/master /data1/primary /data1/mirror && chmod -R 755 /data1 && \
    gpdb_src/concourse/scripts/setup_gpadmin_user.bash && \
    source gpdb_src/concourse/scripts/common.bash  && \
    chown -R gpadmin:gpadmin /data1 && \
    chown -R gpadmin:gpadmin /home/gpadmin && \
    install_gpdb

# Install PXF
ENV OUTPUT_ARTIFACT_DIR="pxf_tarball"
COPY . /home/gpadmin/pxf_src
COPY ./automation/arenadata/scripts/compile_pxf_without_test.sh ./pxf_src/concourse/scripts/compile_pxf_without_test.sh
RUN chmod +x ./pxf_src/concourse/scripts/compile_pxf_without_test.sh
RUN source gpdb_src/concourse/scripts/common.bash && \
    mkdir pxf_tarball && \
    install_gpdb && \
    source '/usr/local/greenplum-db-devel/greenplum_path.sh' && \
    export SKIP_FDW_BUILD_REASON=0 && \
    pxf_src/concourse/scripts/compile_pxf_without_test.sh && \
    chown -R gpadmin:gpadmin /home/gpadmin/pxf_src

RUN mkdir workspace && ln -s /home/gpadmin/pxf_src /home/gpadmin/workspace/pxf && chown -R gpadmin:gpadmin /home/gpadmin/workspace

# Need to run automation tests
ENV PXF_HOME=/usr/local/greenplum-db-devel/pxf
RUN sudo -H -u gpadmin bash -c "pip install --user paramiko"
RUN localedef -c -i ru_RU -f CP1251 ru_RU.CP1251
RUN cp ${PXF_HOME}/templates/*-site.xml ${PXF_HOME}/servers/default/

RUN chmod a+x ./pxf_src/automation/arenadata/scripts/start_adb_cluster.sh
RUN chown -R gpadmin:gpadmin /usr/local/greenplum-db-devel
ENTRYPOINT ["/home/gpadmin/pxf_src/automation/arenadata/scripts/start_adb_cluster.sh"]
