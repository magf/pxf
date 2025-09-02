package org.greenplum.pxf.automation.features.jdbc;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.mysql.Mysql;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

import java.io.File;

import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_NAME;
import static org.greenplum.pxf.automation.PxfTestConstant.PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE;
import static org.testng.Assert.assertEquals;

@Feature("JDBC JSON features")
public class JdbcJsonFeatureTest extends BaseFeature {
    private static final String PXF_MYSQL_SERVER_PROFILE = "mysql";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/mysql/jdbc-site.xml";
    private static final String[] POSTGRES_JSON_TABLE_FIELDS = new String[]{
            "id    int",
            "data_jsonb   jsonb",
            "data_json  json"};

    private static final String[] MYSQL_JSON_TABLE_FIELDS = new String[]{
            "id    int",
            "data_jsonb   json",
            "data_json  json"};
    private static final String MYSQL_CHECK_NULL_QUERY = "SELECT data_json FROM mysql_json_target_table WHERE data_jsonb IS NULL";
    private static final String MYSQL_CHECK_NULL_EXPECTED_VALUE = "[{\"follower\": 15}, {\"names\": \"Bob, Barby\"}]";

    private static final String JSON_FILE_NAME = "jdbc_json.txt";
    private Table gpdbJsonSourceTable;
    private Table postgresJsonTargetTable;
    private Table mysqlJsonTargetTable;
    private Mysql mysql;

    @Override
    protected void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            mysql = (Mysql) SystemManagerImpl.getInstance().getSystemObject("mysql");
            copyJdbcConfFile();
            prepareData();
        }
    }

    @Step("Prepare data")
    protected void prepareData() throws Exception {
        prepareGpdbJsonSourceTable();
        preparePostgresJsonTargetTable();
        createPostgresJsonWritableTable();
        createPostgresJsonReadableTable();
        prepareMysqlJsonTargetTable();
        createMysqlJsonWritableTable();
        createMysqlJsonReadableTable();
    }

    @Step("Prepare GPDB JSON source table")
    private void prepareGpdbJsonSourceTable() throws Exception {
        gpdbJsonSourceTable = new Table("json_source_table", POSTGRES_JSON_TABLE_FIELDS);
        gpdbJsonSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbJsonSourceTable);
        gpdb.copyFromFile(gpdbJsonSourceTable, new File(localDataResourcesFolder
                + "/jdbc/" + JSON_FILE_NAME), null, null, false);
    }

    @Step("Prepare Postgres JSON target table")
    private void preparePostgresJsonTargetTable() throws Exception {
        postgresJsonTargetTable = new Table("postgres_json_target_table", POSTGRES_JSON_TABLE_FIELDS);
        postgresJsonTargetTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(postgresJsonTargetTable);
    }

    @Step("Create Postgres JSON writable table")
    private void createPostgresJsonWritableTable() throws Exception {
        ExternalTable postgresJsonWritableTable = TableFactory.getPxfJdbcWritableTable(
                "postgres_json_write_ext_table",
                POSTGRES_JSON_TABLE_FIELDS,
                postgresJsonTargetTable.getName(),
                "default");
        gpdb.createTableAndVerify(postgresJsonWritableTable);
    }

    @Step("Create Postgres JSON readable table")
    private void createPostgresJsonReadableTable() throws Exception {
        ExternalTable postgresJsonReadableTable = TableFactory.getPxfJdbcReadableTable(
                "postgres_json_read_ext_table",
                POSTGRES_JSON_TABLE_FIELDS,
                gpdbJsonSourceTable.getName(),
                "default");
        gpdb.createTableAndVerify(postgresJsonReadableTable);
    }

    @Step("Prepare MySql JSON target table")
    private void prepareMysqlJsonTargetTable() throws Exception {
        mysqlJsonTargetTable = new Table("mysql_json_target_table", MYSQL_JSON_TABLE_FIELDS);
        mysql.createTableAndVerify(mysqlJsonTargetTable);
    }

    @Step("Create MySql JSON writable table")
    private void createMysqlJsonWritableTable() throws Exception {
        ExternalTable gpdbJsonMysqlWritableTable = TableFactory.getPxfJdbcWritableTable(
                "mysql_json_write_ext_table",
                POSTGRES_JSON_TABLE_FIELDS,
                mysqlJsonTargetTable.getName(),
                "mysql");
        gpdb.createTableAndVerify(gpdbJsonMysqlWritableTable);
    }

    @Step("Create MySql JSON readable table")
    private void createMysqlJsonReadableTable() throws Exception {
        ExternalTable gpdbJsonMysqlReadableTable = TableFactory.getPxfJdbcReadableTable(
                "mysql_json_read_ext_table",
                POSTGRES_JSON_TABLE_FIELDS,
                mysqlJsonTargetTable.getName(),
                "mysql");
        gpdb.createTableAndVerify(gpdbJsonMysqlReadableTable);
    }

    @Test(groups = {"features", "jdbc"})
    public void testPostgresJdbcJson() throws Exception {
        runSqlTest("features/jdbc/jdbc-json/postgres");
    }

    @Test(groups = {"features", "jdbc"})
    public void testMysqlJdbcJson() throws Exception {
        runSqlTest("features/jdbc/jdbc-json/mysql");
        assertEquals((String) mysql.getValueFromQuery(MYSQL_CHECK_NULL_QUERY, 1, String.class), MYSQL_CHECK_NULL_EXPECTED_VALUE);
    }

    private void copyJdbcConfFile() throws Exception {
        String pxfHome = cluster.getPxfHome();
        String pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_MYSQL_SERVER_PROFILE);
        String pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
        String pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
        cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
    }
}
