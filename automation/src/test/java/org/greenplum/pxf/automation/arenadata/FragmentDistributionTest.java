package org.greenplum.pxf.automation.arenadata;

import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.enums.EnumPartitionType;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_LOG_RELATIVE_PATH;
import static org.greenplum.pxf.automation.PxfTestUtil.getCmdResult;
import static org.junit.Assert.assertEquals;

public class FragmentDistributionTest extends BaseFeature {
    private static final String PG_SOURCE_TABLE_NAME = "pg_fragment_distribution_source_table";
    private static final String JDBC_EXT_TABLE_NAME = "fragment_distribution_jdbc_ext_table";
    private static final String JDBC_LIMIT_EXT_TABLE_NAME = "fragment_distribution_jdbc_limit_ext_table";
    private static final String HDFS_EXT_TABLE_NAME = "fragment_distribution_hdfs_ext_table";
    private static final String HDFS_LIMIT_EXT_TABLE_NAME = "fragment_distribution_hdfs_limit_ext_table";
    private static final String HDFS_SEG_LIMIT_EXT_TABLE_NAME = "fragment_distribution_hdfs_seg_limit_ext_table";
    private static final String[] SOURCE_TABLE_FIELDS = new String[]{
            "id    int",
            "descr   text"};
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf";
    private static final String PXF_TEMP_LOG_FILE = PXF_TEMP_LOG_PATH + "/pxf-service.log";
    private static final String DELIMITER = ";";
    private static final String CAT_COMMAND = "cat " + PXF_TEMP_LOG_FILE;
    private static final String PXF_LOG_JDBC_GREP_COMMAND = CAT_COMMAND + " | grep -E 'Returning [1-7]/7 fragment' | wc -l";
    private static final String PXF_LOG_JDBC_GREP_COMMAND_LIMIT = CAT_COMMAND + " | grep 'Returning 0/7 fragment' | wc -l";
    private static final String PXF_LOG_HDFS_GREP_COMMAND = CAT_COMMAND + " | grep 'Returning 1/6 fragment' | wc -l";
    private static final String PXF_LOG_HDFS_COMMAND_LIMIT = CAT_COMMAND + " | grep 'Returning 0/6 fragment' | wc -l";
    private static final String PXF_LOG_HDFS_SEG_COMMAND_LIMIT = CAT_COMMAND + " | grep -E 'Returning [1-3]/6 fragment' | wc -l";
    private List<Node> pxfNodes;
    private String pxfLogFile;
    private String hdfsPath;
    private Table dataTable;
    private Table postgresSourceTable;
    private String restartCommand;

    @Override
    protected void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            String pxfHome = cluster.getPxfHome();
            restartCommand = pxfHome + "/bin/pxf restart";
            if (cluster instanceof MultiNodeCluster) {
                pxfNodes = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf);
            }
            pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
            hdfsPath = hdfs.getWorkingDirectory() + "/parquet_fragment_distribution/";
            prepareData();
            changeLogLevel("debug");
            cluster.runCommand("mkdir -p " + PXF_TEMP_LOG_PATH);
        }
    }

    @Override
    public void afterClass() throws Exception {
        changeLogLevel("info");
    }

    protected void prepareData() throws Exception {
        preparePgSourceTable();
        prepareHdfsFiles();
        createGpdbReadableJdbcTable();
        createGpdbReadableJdbcTableWithLimit();
        createGpdbReadableHdfsTable();
        createGpdbReadableHdfsTableWithLimit();
        createGpdbReadableHdfsTableWithSegLimit();
    }

    private void preparePgSourceTable() throws Exception {
        postgresSourceTable = new Table(PG_SOURCE_TABLE_NAME, SOURCE_TABLE_FIELDS);
        postgresSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(postgresSourceTable);
        // Generate rows for source table
        int count = 20;
        String[][] rows = new String[count][2];
        for (int i = 0; i < count; i++) {
            rows[i][0] = String.valueOf(i + 1);
            rows[i][1] = "text" + (i + 1);
        }
        dataTable = new Table("dataTable", SOURCE_TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, postgresSourceTable);
    }

    private void prepareHdfsFiles() {
        // Create 6 files to receive 6 fragments
        IntStream.rangeClosed(0, 5)
                .forEach(i -> {
                    try {
                        hdfs.writeTableToFile(hdfsPath + "file-" + i, dataTable, DELIMITER);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void createGpdbReadableJdbcTable() throws Exception {
        ExternalTable gpdbReadablePgTable = TableFactory.getPxfJdbcReadablePartitionedTable(
                JDBC_EXT_TABLE_NAME,
                SOURCE_TABLE_FIELDS,
                postgresSourceTable.getName(),
                0,
                "1:20",
                "5",
                EnumPartitionType.INT,
                "default",
                null);
        gpdb.createTableAndVerify(gpdbReadablePgTable);
    }

    private void createGpdbReadableJdbcTableWithLimit() throws Exception {
        ExternalTable gpdbReadablePgTable = TableFactory.getPxfJdbcReadablePartitionedTable(
                JDBC_LIMIT_EXT_TABLE_NAME,
                SOURCE_TABLE_FIELDS,
                postgresSourceTable.getName(),
                0,
                "1:20",
                "5",
                EnumPartitionType.INT,
                "default",
                "ACTIVE_SEGMENT_COUNT=2");
        gpdb.createTableAndVerify(gpdbReadablePgTable);
    }

    private void createGpdbReadableHdfsTable() throws Exception {
        ReadableExternalTable gpdbReadableHdfsTable = TableFactory.getPxfReadableCSVTable(
                HDFS_EXT_TABLE_NAME,
                SOURCE_TABLE_FIELDS,
                hdfsPath,
                DELIMITER);
        gpdb.createTableAndVerify(gpdbReadableHdfsTable);
    }

    private void createGpdbReadableHdfsTableWithLimit() throws Exception {
        ReadableExternalTable gpdbReadableHdfsTable = TableFactory.getPxfReadableCSVTable(
                HDFS_LIMIT_EXT_TABLE_NAME,
                SOURCE_TABLE_FIELDS,
                hdfsPath,
                DELIMITER);
        gpdbReadableHdfsTable.setUserParameters(new String[]{"ACTIVE_SEGMENT_COUNT=2"});
        gpdb.createTableAndVerify(gpdbReadableHdfsTable);
    }

    private void createGpdbReadableHdfsTableWithSegLimit() throws Exception {
        ReadableExternalTable gpdbReadableHdfsTable = TableFactory.getPxfReadableCSVTable(
                HDFS_SEG_LIMIT_EXT_TABLE_NAME,
                SOURCE_TABLE_FIELDS,
                hdfsPath,
                DELIMITER);
        gpdbReadableHdfsTable.setUserParameters(new String[]{"ACTIVE_SEGMENT_COUNT=4"});
        gpdb.createTableAndVerify(gpdbReadableHdfsTable);
    }

    @Test(groups = {"arenadata"}, description = "Check JDBC fragments distribution across segments. Run on all segments.")
    public void testFragmentDistributionForJdbc() throws Exception {
        cleanLogs();
        runSqlTest("arenadata/fragment-distribution/jdbc");
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            String result = getCmdResult(cluster, PXF_LOG_JDBC_GREP_COMMAND);
            assertEquals("3", result);
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
    }

    @Test(groups = {"arenadata"}, description = "Check JDBC fragments distribution across segments. Run on limit segments")
    public void testFragmentDistributionForJdbcWithLimit() throws Exception {
        cleanLogs();
        runSqlTest("arenadata/fragment-distribution/jdbc-limit");
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            String result = getCmdResult(cluster, PXF_LOG_JDBC_GREP_COMMAND_LIMIT);
            assertEquals("2", result);
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
    }

    @Test(groups = {"arenadata"}, description = "Check HDFS fragments distribution across segments. Run on all segments.")
    public void testFragmentDistributionForHdfs() throws Exception {
        cleanLogs();
        runSqlTest("arenadata/fragment-distribution/hdfs");
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            String result = getCmdResult(cluster, PXF_LOG_HDFS_GREP_COMMAND);
            assertEquals("3", result);
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
    }

    @Test(groups = {"arenadata"}, description = "Check HDFS fragments distribution across segments. Run on limit segments.")
    public void testFragmentDistributionForHdfsWithLimit() throws Exception {
        cleanLogs();
        runSqlTest("arenadata/fragment-distribution/hdfs-limit");
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            String result = getCmdResult(cluster, PXF_LOG_HDFS_COMMAND_LIMIT);
            assertEquals("2", result);
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
    }

    @Test(groups = {"arenadata"}, description = "Check HDFS fragments distribution across segments. Run on 4 segments.")
    public void testFragmentDistributionForHdfsWithSegLimit() throws Exception {
        cleanLogs();
        runSqlTest("arenadata/fragment-distribution/hdfs-seg-limit");
        int activeSegmentCount = 0;
        int idleSegmentCount = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            copyLogs(getMethodName(), pxfNode.getHost());
            activeSegmentCount += Integer.parseInt(getCmdResult(cluster, PXF_LOG_HDFS_SEG_COMMAND_LIMIT));
            idleSegmentCount += Integer.parseInt(getCmdResult(cluster, PXF_LOG_HDFS_COMMAND_LIMIT));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        assertEquals(4, activeSegmentCount);
        assertEquals(2, idleSegmentCount);
    }

    private void changeLogLevel(String level) throws Exception {
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_LOG_LEVEL=%s;%s", level, restartCommand));
    }

    private void cleanLogs() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
    }

    private void copyLogs(String methodName, String host) throws Exception {
        cluster.runCommand("cp " + PXF_TEMP_LOG_FILE + " " + PXF_TEMP_LOG_FILE + "-" + methodName + "-" + host);
    }

    private String getMethodName() throws Exception {
        return Thread.currentThread()
                .getStackTrace()[2]
                .getMethodName();
    }
}
