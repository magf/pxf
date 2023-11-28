package org.greenplum.pxf.automation.arenadata;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;

public class OracleDateTypeMapTest extends BaseFeature {
    private static final String ORACLE_SOURCE_TABLE_NAME = "date_type_source_table";
    private static final String PXF_ORACLE_SERVER_PROFILE = "oracle-date-map";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/oracle/jdbc-site.xml";
    private static final String[] READABLE_EXTERNAL_TABLE_FIELDS = new String[]{
            "id    int",
            "v_date   timestamp",
            "ts   timestamp"};
    private static final String[] ORACLE_SOURCE_TABLE_FIELDS = new String[]{
            "id   int",
            "v_date    date",
            "ts    timestamp"};

    private static final String ORACLE_INSERT_QUERY = "INSERT INTO " + ORACLE_SOURCE_TABLE_NAME +
            " VALUES (1, to_date('2022-01-01 14:00:00', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('2022-02-01 12:01:00.777777', 'YYYY-MM-DD HH24:MI:SS.FF'))";
    private Oracle oracle;
    private Table oracleDateTypeSourceTable;

    @Override
    protected void beforeClass() throws Exception {
        String pxfHome = cluster.getPxfHome();
        String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_ORACLE_SERVER_PROFILE);
        String pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
        cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
        oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject("oracle");
        prepareData();
    }

    protected void prepareData() throws Exception {
        prepareOracleDateTypeSourceTable();
        createGpdbReadableOracleTable("date_type_without_mapping_ext_table", "false");
        createGpdbReadableOracleTable("date_type_with_mapping_ext_table", "true");
    }

    private void prepareOracleDateTypeSourceTable() throws Exception {
        oracleDateTypeSourceTable = new Table(ORACLE_SOURCE_TABLE_NAME, ORACLE_SOURCE_TABLE_FIELDS);
        oracleDateTypeSourceTable.setSchema("system");
        oracle.createTableAndVerify(oracleDateTypeSourceTable);
        oracle.runQuery(ORACLE_INSERT_QUERY);
    }

    private void createGpdbReadableOracleTable(String tableName, String convertOracleDate) throws Exception {
        ExternalTable gpdbReadableOracleTable = TableFactory.getPxfJdbcReadableTable(
                tableName,
                READABLE_EXTERNAL_TABLE_FIELDS,
                oracleDateTypeSourceTable.getSchema() + "." + oracleDateTypeSourceTable.getName(),
                PXF_ORACLE_SERVER_PROFILE);
        gpdbReadableOracleTable.setUserParameters(new String[]{"CONVERT_ORACLE_DATE=" + convertOracleDate});
        gpdb.createTableAndVerify(gpdbReadableOracleTable);
    }

    @Test(groups = {"arenadata"}, description = "Check mapping to Oracle date type")
    public void testOracleDateTypeMapping() throws Exception {
        runTincTest("pxf.arenadata.oracle-date-type-mapping.runTest");
    }
}
