package org.greenplum.pxf.automation.arenadata;

import annotations.WorksWithFDW;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_LOG_RELATIVE_PATH;
import static org.greenplum.pxf.automation.PxfTestUtil.getCmdResult;
import static org.junit.Assert.assertTrue;

@WorksWithFDW
@Feature("Reading HDFS files from Hive Metastore")
public class HiveMetastoreHdfsReadTest extends BaseFeature {
    private static final String[] HIVE_TABLE_FIELDS = {
            "location   STRING",
            "month STRING",
            "number_of_orders   INT",
            "total_sales   DOUBLE"
    };
    private static final String[] EXT_TABLE_FIELDS = {
            "location   TEXT",
            "month TEXT",
            "number_of_orders   INT",
            "total_sales   FLOAT8"
    };
    private static final String SELECT_QUERY = "SELECT * FROM ${pxf_read_table}";
    private static final String SOURCE_TEXT_TABLE_NAME = "sales_info";
    private static final String SOURCE_PARQUET_TABLE_NAME = "hive_parquet_table";
    private static final String PXF_TABLE_NAME = SOURCE_PARQUET_TABLE_NAME + "_ext";
    private static final String PXF_TEMP_LOG_PATH = "/tmp/pxf";
    private static final String PXF_TEMP_LOG_FILE = PXF_TEMP_LOG_PATH + "/pxf-service.log";
    private List<Node> pxfNodes;
    private String pxfLogFile;

    @Override
    protected void beforeClass() throws Exception {
        pxfNodes = ((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf);
        String pxfHome = cluster.getPxfHome();
        pxfLogFile = pxfHome + "/" + PXF_LOG_RELATIVE_PATH;
        String restartCommand = pxfHome + "/bin/pxf restart";
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_LOG_LEVEL=%s;%s", "debug", restartCommand));
        cluster.runCommand("mkdir -p " + PXF_TEMP_LOG_PATH);
    }

    @BeforeMethod
    protected void beforeMethod() throws Exception {
        cleanLogs();
    }

    @AfterMethod
    protected void afterMethod() throws Exception {
        checkPxfLogs("Returning 1/1 fragment");
    }


    @Test(groups = {"arenadata"}, description = "Check PXF support for reading HDFS files from Hive metastore")
    public void testPxfReadHDFSFilesHiveMetastore() throws Exception {
        String srcPath = "/tmp/test_data/hive/pxf_hive_datafile.txt";
        Hive hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");

        HiveTable textHiveTable = TableFactory.getHiveByRowCommaTable(SOURCE_TEXT_TABLE_NAME, HIVE_TABLE_FIELDS);
        textHiveTable.setStoredAs("textfile");
        hive.dropTable(textHiveTable, false);
        hive.createTableAndVerify(textHiveTable);
        hive.loadData(textHiveTable, srcPath, true);
        hive.runQuery(SELECT_QUERY.replace("${pxf_read_table}", "default." + SOURCE_TEXT_TABLE_NAME));

        HiveTable parquetHiveTable = TableFactory.getHiveByRowCommaTable(SOURCE_PARQUET_TABLE_NAME, HIVE_TABLE_FIELDS);
        parquetHiveTable.setStoredAs("parquet");
        hive.dropTable(parquetHiveTable, false);
        hive.createTableAndVerify(parquetHiveTable);
        hive.insertData(textHiveTable, parquetHiveTable);

        ExternalTable externalTable = createExternalTable();
        gpdb.createTableAndVerify(externalTable);

        gpdb.runQuery(SELECT_QUERY.replace("${pxf_read_table}", PXF_TABLE_NAME));
/*        checkPxfLogs("Returning 1/1 fragment");
        checkPxfLogs("Creating accessor 'org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor' " +
                "and resolver 'org.greenplum.pxf.plugins.hdfs.ParquetResolver");*/
    }


    @Step("Create pxf external hive table")
    private ExternalTable createExternalTable() {
        ExternalTable pxfExtTable = new ReadableExternalTable(
                PXF_TABLE_NAME,
                EXT_TABLE_FIELDS,
                "default."+ SOURCE_PARQUET_TABLE_NAME,
                "CUSTOM");
        pxfExtTable.setFormatter("pxfwritable_import");
        pxfExtTable.setProfile("hive_parquet_custom");
        pxfExtTable.setServer("server=default");
        pxfExtTable.setHost(pxfHost);
        pxfExtTable.setPort(pxfPort);
        return pxfExtTable;
    }

    @Step("Check that partitioning logs are present")
    private void checkPxfLogs(String searchedLog) throws Exception {
        String greppedLog = "cat " + PXF_TEMP_LOG_FILE + " | grep \"" + searchedLog + "\" | wc -l";
        int result = 0;
        for (Node pxfNode : pxfNodes) {
            cluster.copyFromRemoteMachine(pxfNode.getUserName(), pxfNode.getPassword(), pxfNode.getHost(), pxfLogFile, PXF_TEMP_LOG_PATH);
            cluster.runCommand("cp " + PXF_TEMP_LOG_FILE + " " + PXF_TEMP_LOG_FILE + "-" + getMethodName() + "-" + pxfNode.getHost());
            result += Integer.parseInt(getCmdResult(cluster, greppedLog));
            cluster.deleteFileFromNodes(PXF_TEMP_LOG_FILE, false);
        }
        //assertTrue("Check that log is present at least once on one of segment hosts", result > 0);
    }

    private void cleanLogs() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, "> " + pxfLogFile);
    }

    private String getMethodName() throws Exception {
        return Thread.currentThread()
                .getStackTrace()[2]
                .getMethodName();
    }
}
