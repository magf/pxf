package org.greenplum.pxf.automation.arenadata;

import io.qameta.allure.Feature;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.JdbcDbType;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.components.mysql.Mysql;
import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static org.greenplum.pxf.automation.JdbcDbType.*;
import static org.greenplum.pxf.automation.PxfTestConstant.*;
import static org.greenplum.pxf.automation.PxfTestUtil.getCmdResult;
import static org.junit.Assert.assertEquals;

@Feature("JDBC Timestamp partitioning")
public class JdbcTimestampPartitioningTest extends BaseFeature {

    private static final String[] TABLE_FIELDS = new String[]{
            "id    int",
            "datetime   timestamp"};
    private static final String SELECT_QUERY = "SELECT * FROM ${pxf_read_table}";
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf-service.log";
    private static final String SEGMENT_LOG_GREP_COMMAND = "cat " + PXF_TEMP_LOG_PATH + " | grep 'Returning 14 fragments' | wc -l";
    private static final String SOURCE_TABLE_NAME = "source_table";
    private List<Node> pxfNodes;
    private String pxfLogFile;

    @Override
    protected void beforeClass() throws Exception {
        pxfNodes = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf);
        String pxfHome = cluster.getPxfHome();
        String restartCommand = pxfHome + "/bin/pxf restart";
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_LOG_LEVEL=%s;%s", "trace", restartCommand));
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
    }

    @Test(groups = {"arenadata"}, dataProvider = "useJdbcTimestampPartitioningProvider")
    public void testTimestampPartitioning(String pxfReadTableName, JdbcDbType jdbcDbType) throws Exception {
        cleanLogs();
        prepareSourceTable(jdbcDbType);
        ExternalTable pxfReadTable = createExternalTable(pxfReadTableName,
                SOURCE_TABLE_NAME,
                jdbcDbType.getServer());
        copyJdbcConfFile(jdbcDbType);
        gpdb.createTableAndVerify(pxfReadTable);
        gpdb.runQuery(SELECT_QUERY.replace("${pxf_read_table}", pxfReadTableName));
        checkPxfLogs();
    }

    @DataProvider
    private Object[][] useJdbcTimestampPartitioningProvider() {
        return new Object[][]{
                {"oracle_ext_table", ORACLE},
                {"mysql_ext_table", MYSQL},
                {"postgres_ext_table", DEFAULT}
        };
    }

    private void cleanLogs() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
    }

    private void prepareSourceTable(JdbcDbType jdbcDbType) throws Exception {
        switch (jdbcDbType) {
            case ORACLE:
                Oracle oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject(ORACLE.getServer());
                Table oracleSrcTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
                oracleSrcTable.setSchema("system");
                oracle.createTableAndVerify(oracleSrcTable);
                oracle.runQuery(getInsertQuery(jdbcDbType));
            case MYSQL:
                Mysql mysql = (Mysql) SystemManagerImpl.getInstance().getSystemObject(MYSQL.getServer());
                Table mysqlSrcTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
                mysql.createTableAndVerify(mysqlSrcTable);
                mysql.runQuery(getInsertQuery(jdbcDbType));
            default:
                Table gpdbSrcTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
                gpdbSrcTable.setDistributionFields(new String[]{"id"});
                gpdb.createTableAndVerify(gpdbSrcTable);
                gpdb.runQuery(getInsertQuery(jdbcDbType));
        }
    }

    private String getInsertQuery(JdbcDbType jdbcDbType) {
        if (jdbcDbType == ORACLE) {
            return "INSERT ALL " +
                    "INTO source_table values(1, TIMESTAMP'2023-01-01 10:00:00') " +
                    "INTO source_table VALUES (1, TIMESTAMP'2023-01-01 10:00:00') " +
                    "INTO source_table VALUES (2, TIMESTAMP'2023-01-02 11:15:00') " +
                    "INTO source_table VALUES (3, TIMESTAMP'2023-01-03 12:30:00') " +
                    "INTO source_table VALUES (4, TIMESTAMP'2023-01-04 13:45:00') " +
                    "INTO source_table VALUES (5, TIMESTAMP'2023-01-05 15:00:00') " +
                    "INTO source_table VALUES (6, TIMESTAMP'2023-01-06 16:15:00') " +
                    "INTO source_table VALUES (7, TIMESTAMP'2023-01-07 17:30:00') " +
                    "INTO source_table VALUES (8, TIMESTAMP'2023-01-08 18:45:00') " +
                    "INTO source_table VALUES (9, TIMESTAMP'2023-01-09 20:00:00') " +
                    "INTO source_table VALUES (10, TIMESTAMP'2023-01-10 21:15:00') " +
                    "SELECT * FROM DUAL";
        } else {
            return "INSERT INTO source_table VALUES " +
                    "(1, '2023-01-01 10:00:00'), " +
                    "(2, '2023-01-02 11:15:00'), " +
                    "(3, '2023-01-03 12:30:00'), " +
                    "(4, '2023-01-04 13:45:00'), " +
                    "(5, '2023-01-05 15:00:00'), " +
                    "(6, '2023-01-06 16:15:00'), " +
                    "(7, '2023-01-07 17:30:00'), " +
                    "(8, '2023-01-08 18:45:00'), " +
                    "(9, '2023-01-09 20:00:00'), " +
                    "(10, '2023-01-10 21:15:00');";
        }
    }

    private ExternalTable createExternalTable(String pxfExternalTableName, String targetTableName, String pxfServerName) {
        ExternalTable pxfMysqlReadableTable = TableFactory.getPxfJdbcReadableTable(
                pxfExternalTableName,
                TABLE_FIELDS,
                targetTableName,
                pxfServerName);
        pxfMysqlReadableTable.addUserParameter("PARTITION_BY=datetime:TIMESTAMP");
        pxfMysqlReadableTable.addUserParameter("RANGE=20240101T124910:20240101T125001");
        pxfMysqlReadableTable.addUserParameter("INTERVAL=5:second");
        return pxfMysqlReadableTable;
    }

    private void copyJdbcConfFile(JdbcDbType jdbcDbType) throws Exception {
        if (jdbcDbType != DEFAULT) {
            String pxfHome = cluster.getPxfHome();
            String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, jdbcDbType.getServer());
            String pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
            String jdbcTemplatePath = "templates/${jdbcServer}/jdbc-site.xml";
            String pxfJdbcSiteConfTemplate = pxfHome + "/" + jdbcTemplatePath.replace("${jdbcServer}", jdbcDbType.getServer());
            cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
            cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
        }
    }


    private void checkPxfLogs() throws Exception {
        int result = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, "/tmp/");
            result += Integer.parseInt(getCmdResult(cluster, SEGMENT_LOG_GREP_COMMAND));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_PATH, false);
        }
        assertEquals(2, result);
    }

}
