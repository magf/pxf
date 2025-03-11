package org.greenplum.pxf.automation.arenadatassl;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.components.cluster.MultiNodeCluster;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.CoordinatorNode;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.Node;
import org.greenplum.pxf.automation.components.cluster.installer.nodes.SegmentNode;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ForeignTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.postgresql.util.PSQLException;
import org.testng.annotations.Test;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThrows;

@WorksWithFDW
@Feature("PXF Vault Integration")
public class VaultIntegrationTest extends BaseFeature {
    private static final String SOURCE_TABLE_NAME = "source";
    private static final String PXF_TABLE_NAME = "jdbc_fdw_table";
    private static final String[] TABLE_FIELDS = {
            "id INT",
            "username TEXT"
    };
    private static final String INSERT_QUERY = "INSERT INTO " + SOURCE_TABLE_NAME + " (id, username) VALUES " +
            "(1, 'Oliver'), " +
            "(2, 'Lisa'), " +
            "(3, 'Gabri'), " +
            "(4, 'Milisa'), " +
            "(5, 'Kostas'), " +
            "(6, 'Ivan'), " +
            "(7, 'Greg'), " +
            "(8, 'Alex'), " +
            "(10, 'Sergio');";

    private String pxfHome;
    private String restartCommand;
    private List<Node> pxfNodes;

    @Override
    protected void beforeClass() throws Exception {
        pxfHome = cluster.getPxfHome();
        restartCommand = pxfHome + "/bin/pxf restart";
        pxfNodes = new ArrayList<>();
        pxfNodes.addAll(((MultiNodeCluster) cluster).getNode(SegmentNode.class, PhdCluster.EnumClusterServices.pxf));
        pxfNodes.add(((MultiNodeCluster) cluster).getNode(CoordinatorNode.class, PhdCluster.EnumClusterServices.pxf).get(0));
    }

    @Test(groups = {"arenadatassl"})
    public void testVaultPxfSslEnabledFalse() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_SSL_ENABLED=%s;export PXF_PROTOCOL=%s;%s", "false", "http", restartCommand));
        Table sourceTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
        sourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(sourceTable);
        gpdb.runQuery(INSERT_QUERY);
        ForeignTable pxfForeignTable = (ForeignTable) TableFactory.getPxfJdbcWritableTable(
                PXF_TABLE_NAME,
                TABLE_FIELDS,
                SOURCE_TABLE_NAME,
                "server=default"
        );
        pxfForeignTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(pxfForeignTable);
        gpdb.runQuery("SELECT * FROM " + PXF_TABLE_NAME);
    }

    @Test(groups = {"arenadatassl"})
    public void testVaultPxfSslEnabledTrue() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_SSL_ENABLED=%s;export PXF_PROTOCOL=%s;%s", "true", "https", restartCommand));
        Table sourceTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
        sourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(sourceTable);
        gpdb.runQuery(INSERT_QUERY);
        ForeignTable pxfForeignTable = (ForeignTable) TableFactory.getPxfJdbcWritableTable(
                PXF_TABLE_NAME,
                TABLE_FIELDS,
                SOURCE_TABLE_NAME,
                ""
        );
        pxfForeignTable.setServer("default_pxf_server_ssl");
        pxfForeignTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(pxfForeignTable);
        gpdb.runQuery("SELECT * FROM " + PXF_TABLE_NAME);
    }

    @Test(groups = {"arenadatassl"})
    public void testVaultPxfSslEnabledTrueNoSslWrapperError() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_SSL_ENABLED=%s;export PXF_PROTOCOL=%s;%s", "true", "https", restartCommand));
        Table sourceTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
        sourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(sourceTable);
        gpdb.runQuery(INSERT_QUERY);
        ForeignTable pxfForeignTable = (ForeignTable) TableFactory.getPxfJdbcWritableTable(
                PXF_TABLE_NAME,
                TABLE_FIELDS,
                SOURCE_TABLE_NAME,
                "server=default"
        );
        pxfForeignTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(pxfForeignTable);
        assertThrows("This combination of host and port requires TLS",
                PSQLException.class, () -> gpdb.runQuery("SELECT * FROM " + PXF_TABLE_NAME));
    }

    @Test(groups = {"arenadatassl"})
    public void testVaultPxfSslEnabledFalseWithSslWrapperError() throws Exception {
        cluster.runCommandOnNodes(pxfNodes, String.format("export PXF_SSL_ENABLED=%s;export PXF_PROTOCOL=%s;%s", "false", "http", restartCommand));
        Table sourceTable = new Table(SOURCE_TABLE_NAME, TABLE_FIELDS);
        sourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(sourceTable);
        gpdb.runQuery(INSERT_QUERY);
        ForeignTable pxfForeignTable = (ForeignTable) TableFactory.getPxfJdbcWritableTable(
                PXF_TABLE_NAME,
                TABLE_FIELDS,
                SOURCE_TABLE_NAME,
                ""
        );
        pxfForeignTable.setServer("default_pxf_server_ssl");
        pxfForeignTable.setUserParameters(new String[]{"date_wide_range=true"});
        gpdb.createTableAndVerify(pxfForeignTable);
        assertThrows("SSL routines::wrong version number", PSQLException.class, () -> gpdb.runQuery("SELECT * FROM " + PXF_TABLE_NAME));
    }
}
