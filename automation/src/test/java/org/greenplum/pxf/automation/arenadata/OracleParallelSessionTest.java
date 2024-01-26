package org.greenplum.pxf.automation.arenadata;

import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import static org.greenplum.pxf.automation.PxfTestConstant.*;
import static org.testng.Assert.assertEquals;

public class OracleParallelSessionTest extends BaseFeature {
    private static final String PXF_ORACLE_SERVER_PROFILE = "oracle-parallel";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/oracle/jdbc-site.xml";
    private static final String INSERT_QUERY = "INSERT ALL\n" +
            "INTO system.oracle_parallel_source_table VALUES (1, 'text1')\n" +
            "INTO system.oracle_parallel_source_table VALUES (2, 'text2')\n" +
            "INTO system.oracle_parallel_source_table VALUES (3, 'text3')\n" +
            "INTO system.oracle_parallel_source_table VALUES (4, 'text4')\n" +
            "INTO system.oracle_parallel_source_table VALUES (5, 'text5')\n" +
            "SELECT 1 FROM DUAL";
    private static final String GET_STATS_QUERY_TEMPLATE = "SELECT * FROM (SELECT px_servers_executions FROM v$sqlstats \n" +
            "WHERE SQL_FULLTEXT LIKE '%%%s%%' \n" +
            "AND SQL_FULLTEXT NOT LIKE '%%LAST_ACTIVE_TIME%%' \n" +
            "ORDER BY LAST_ACTIVE_TIME DESC) WHERE ROWNUM = 1";
    private static final String FORCE_QUERY_3_PROPERTY = "<property>" +
            "<name>jdbc.session.property.alter_session_parallel.1<\\/name>" +
            "<value>force.query.3<\\/value>" +
            "<\\/property>";
    private static final String DISABLE_QUERY_PROPERTY = "<property>" +
            "<name>jdbc.session.property.alter_session_parallel.1<\\/name>" +
            "<value>disable.query<\\/value>" +
            "<\\/property>";
    private static final String EMPTY_PROPERTY = "\\ \\";

    private static final String[] ORACLE_SOURCE_TABLE_FIELDS = new String[]{
            "id    NUMBER",
            "descr   VARCHAR2(100)"};

    private static final String[] GPDB_TABLE_FIELDS = new String[]{
            "id    int",
            "descr   text"};

    private Table oracleTableSource;
    private String pxfHome;
    private String pxfJdbcSiteConfPath;
    private String pxfJdbcSiteConfFile;
    private String pxfJdbcSiteConfTemplate;
    private Oracle oracle;

    @Override
    public void beforeClass() throws Exception {
        pxfHome = cluster.getPxfHome();
        pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_ORACLE_SERVER_PROFILE);
        pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
        pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
        oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject("oracle");
        prepareData();
    }

    protected void prepareData() throws Exception {
        prepareOracleSourceTable();
        createGpdbReadableTable();
    }

    private void prepareOracleSourceTable() throws Exception {
        oracleTableSource = new Table("oracle_parallel_source_table", ORACLE_SOURCE_TABLE_FIELDS);
        oracleTableSource.setSchema("system");
        oracle.createTableAndVerify(oracleTableSource);
        oracle.runQuery(INSERT_QUERY);
    }

    private void createGpdbReadableTable() throws Exception {
        Table gpdbReadableTable = TableFactory.getPxfJdbcReadableTable(
                "oracle_parallel_read_ext_table",
                GPDB_TABLE_FIELDS,
                oracleTableSource.getSchema() + "." + oracleTableSource.getName(),
                PXF_ORACLE_SERVER_PROFILE);
        gpdb.createTableAndVerify(gpdbReadableTable);
    }

    @Test(groups = {"arenadata"}, description = "Set default parameters for parallel queries")
    public void checkDefaultParamsForParallel() throws Exception {
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, EMPTY_PROPERTY);
        runSqlTest("pxf.arenadata.oracle-parallel.query.runTest");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + oracleTableSource.getSchema() + "." + oracleTableSource.getName())), 0
        );
    }

    @Test(groups = {"arenadata"}, description = "Set disable parallel for query", dependsOnMethods = {"checkDefaultParamsForParallel"})
    public void checkDisableQueryParallel() throws Exception {
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, DISABLE_QUERY_PROPERTY);
        runSqlTest("pxf.arenadata.oracle-parallel.query.runTest");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + oracleTableSource.getSchema() + "." + oracleTableSource.getName())), 0
        );
    }

    @Test(groups = {"arenadata"}, description = "Set 3 parallel sessions with force query", dependsOnMethods = {"checkDisableQueryParallel"})
    public void checkForceQueryWith3Parallel() throws Exception {
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, FORCE_QUERY_3_PROPERTY);
        runSqlTest("pxf.arenadata.oracle-parallel.query.runTest");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + oracleTableSource.getSchema() + "." + oracleTableSource.getName())), 3
        );
    }

    private void copyAndModifyJdbcConfFile(String templateSource, String property) throws Exception {
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
        cluster.copyFileToNodes(templateSource, pxfJdbcSiteConfPath, true, false);
        cluster.runCommandOnAllNodes("sed -i '/<\\/configuration>/i " + property + "' " + pxfJdbcSiteConfFile);
    }
}
