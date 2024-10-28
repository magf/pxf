package org.greenplum.pxf.automation.arenadata;

import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.oracle.Oracle;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

import static org.greenplum.pxf.automation.PxfTestConstant.*;
import static org.testng.Assert.assertEquals;

public class OracleParallelSessionTest extends BaseFeature {
    private static final String PXF_ORACLE_SERVER_PROFILE = "oracle-parallel";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/oracle/jdbc-site.xml";
    private static final String INSERT_QUERY_TEMPLATE = "INSERT ALL\n" +
            "INTO %s VALUES (1, 'text1')\n" +
            "INTO %s VALUES (2, 'text2')\n" +
            "INTO %s VALUES (3, 'text3')\n" +
            "INTO %s VALUES (4, 'text4')\n" +
            "INTO %s VALUES (5, 'text5')\n" +
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

    private String pxfJdbcSiteConfPath;
    private String pxfJdbcSiteConfFile;
    private String pxfJdbcSiteConfTemplate;
    private Oracle oracle;

    @Override
    public void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            String pxfHome = cluster.getPxfHome();
            pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_ORACLE_SERVER_PROFILE);
            pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
            pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
            oracle = (Oracle) SystemManagerImpl.getInstance().getSystemObject("oracle");
        }
    }

    @Test(groups = {"arenadata"}, description = "Set default parameters for parallel queries")
    public void checkDefaultParamsForParallel() throws Exception {
        String oracleTableSourceName = "default_source_table";
        Table oracleTableSource = prepareOracleSourceTable(oracleTableSourceName);
        String dataSourcePath = oracleTableSource.getSchema() + "." + oracleTableSource.getName();
        createGpdbReadableTable("oracle_parallel_default_read_ext", dataSourcePath);
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, EMPTY_PROPERTY);
        runSqlTest("arenadata/oracle-parallel/default");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + dataSourcePath)), 0
        );
    }

    @Test(groups = {"arenadata"}, description = "Set disable parallel for query")
    public void checkDisableQueryParallel() throws Exception {
        String oracleTableSourceName = "disable_query_source_table";
        Table oracleTableSource = prepareOracleSourceTable(oracleTableSourceName);
        String dataSourcePath = oracleTableSource.getSchema() + "." + oracleTableSource.getName();
        createGpdbReadableTable("oracle_parallel_disable_query_read_ext", dataSourcePath);
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, DISABLE_QUERY_PROPERTY);
        runSqlTest("arenadata/oracle-parallel/disable-query");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + dataSourcePath)), 0
        );
    }

    @Test(groups = {"arenadata"}, description = "Set 3 parallel sessions with force query")
    public void checkForceQueryWith3Parallel() throws Exception {
        String oracleTableSourceName = "force_query_3_source_table";
        Table oracleTableSource = prepareOracleSourceTable(oracleTableSourceName);
        String dataSourcePath = oracleTableSource.getSchema() + "." + oracleTableSource.getName();
        createGpdbReadableTable("oracle_parallel_force_query_3_read_ext", dataSourcePath);
        copyAndModifyJdbcConfFile(pxfJdbcSiteConfTemplate, FORCE_QUERY_3_PROPERTY);
        runSqlTest("arenadata/oracle-parallel/force-query");
        assertEquals(oracle.getValueFromQuery(
                String.format(GET_STATS_QUERY_TEMPLATE, "SELECT id, descr FROM " + dataSourcePath)), 3
        );
    }

    private Table prepareOracleSourceTable(String tableName) throws Exception {
        String schema = "system";
        Table oracleTableSource = new Table(tableName, ORACLE_SOURCE_TABLE_FIELDS);
        oracleTableSource.setSchema(schema);
        oracle.createTableAndVerify(oracleTableSource);
        String ds = schema + "." + tableName;
        oracle.runQuery(String.format(INSERT_QUERY_TEMPLATE, ds, ds, ds, ds, ds));
        return oracleTableSource;
    }

    private void createGpdbReadableTable(String readableTableName, String dataSourcePath) throws Exception {
        Table gpdbReadableTable = TableFactory.getPxfJdbcReadableTable(
                readableTableName,
                GPDB_TABLE_FIELDS,
                dataSourcePath,
                PXF_ORACLE_SERVER_PROFILE);
        gpdb.createTableAndVerify(gpdbReadableTable);
    }

    private void copyAndModifyJdbcConfFile(String templateSource, String property) throws Exception {
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
        cluster.copyFileToNodes(templateSource, pxfJdbcSiteConfPath, true, false);
        cluster.runCommandOnAllNodes("sed -i '/<\\/configuration>/i " + property + "' " + pxfJdbcSiteConfFile);
    }
}
