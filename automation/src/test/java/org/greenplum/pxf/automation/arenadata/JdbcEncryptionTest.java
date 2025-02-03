package org.greenplum.pxf.automation.arenadata;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.annotations.Test;

import static org.greenplum.pxf.automation.PxfTestConstant.*;

@Feature("JDBC encryption")
public class JdbcEncryptionTest extends BaseFeature {
    private static final String PXF_ENCRYPTION_SERVER_PROFILE = "encryption";
    private static final String PASSWORD = "jdbc_user_password";
    private static final String ENCRYPTED_PASSWORD = "aes256:e77OEIQcMjH0nVS7dTGg4Ow00CVYE9jKxA+EPns5wfm1oA3sxY00pg0wCpWDQrobiysiFoB80GOxeLUd69WXhA==";
    private static final String WRONG_ENCRYPTED_PASSWORD = "aes256:wrong-password";
    private static final String ADD_ENCRYPTION_PROPERTIES_COMMAND_TEMPLATE = "cat <<EOT >> %s/conf/pxf-application.properties\n" +
            "\n# Encryption\n" +
            "pxf.ssl.jks-store.path=%s/conf/pxfkeystore.jks\n" +
            "pxf.ssl.jks-store.password=12345678\n" +
            "pxf.ssl.salt.key=PXF_PASS_KEY\n" +
            "EOT";
    private static final String[] SOURCE_TABLE_FIELDS = new String[]{
            "id    int",
            "descr   text"};
    private static final String CREATE_USER_QUERY = "DROP ROLE IF EXISTS jdbc_user;" +
            "CREATE ROLE jdbc_user SUPERUSER LOGIN PASSWORD '" + PASSWORD + "'";
    private static final String PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH = "templates/encryption/jdbc-site.xml";

    private String pxfJdbcSiteConfPath;
    private String pxfJdbcSiteConfFile;
    private String pxfJdbcSiteConfTemplate;
    private Table gpdbEncryptionSourceTable;

    @Override
    public void beforeClass() throws Exception {
        if (!FDWUtils.useFDW) {
            String pxfHome = cluster.getPxfHome();
            pxfJdbcSiteConfPath = String.format(PXF_JDBC_SITE_CONF_FILE_PATH_TEMPLATE, pxfHome, PXF_ENCRYPTION_SERVER_PROFILE);
            pxfJdbcSiteConfFile = pxfJdbcSiteConfPath + "/" + PXF_JDBC_SITE_CONF_FILE_NAME;
            pxfJdbcSiteConfTemplate = pxfHome + "/" + PXF_JDBC_SITE_CONF_TEMPLATE_RELATIVE_PATH;
            gpdb.runQuery(CREATE_USER_QUERY, true, false);
            cluster.runCommandOnAllNodes(String.format(ADD_ENCRYPTION_PROPERTIES_COMMAND_TEMPLATE, pxfHome, pxfHome));
            cluster.restart(PhdCluster.EnumClusterServices.pxf);
            prepareData();
        }
    }

    @Step("Prepare data")
    protected void prepareData() throws Exception {
        prepareSourceTable();
        createGpdbReadableTable();
    }

    @Step("Prepare source table")
    private void prepareSourceTable() throws Exception {
        gpdbEncryptionSourceTable = new Table("encryption_source_table", SOURCE_TABLE_FIELDS);
        gpdbEncryptionSourceTable.setDistributionFields(new String[]{"id"});
        gpdb.createTableAndVerify(gpdbEncryptionSourceTable);
        String[][] rows = new String[][]{
                {"1", "text1"},
                {"2", "text2"},
                {"3", "text3"}};
        Table dataTable = new Table("dataTable", SOURCE_TABLE_FIELDS);
        dataTable.addRows(rows);
        gpdb.insertData(dataTable, gpdbEncryptionSourceTable);
    }

    @Step("Create GPDB readable table")
    private void createGpdbReadableTable() throws Exception {
        Table gpdbReadableTable = TableFactory.getPxfJdbcReadableTable(
                "encryption_ext_table",
                SOURCE_TABLE_FIELDS,
                gpdbEncryptionSourceTable.getName(),
                PXF_ENCRYPTION_SERVER_PROFILE);
        gpdb.createTableAndVerify(gpdbReadableTable);
    }

    @Test(groups = {"arenadata"}, description = "Check wrong encrypted password")
    public void checkWrongEncryptedPassword() throws Exception {
        copyAndModifyJdbcConfFile(WRONG_ENCRYPTED_PASSWORD);
        runSqlTest("arenadata/encryption/error-encrypted");
    }

    @Test(groups = {"arenadata"}, description = "Check encrypted password")
    public void checkEncryptedPassword() throws Exception {
        copyAndModifyJdbcConfFile(ENCRYPTED_PASSWORD);
        runSqlTest("arenadata/encryption/success-encrypted");
    }

    @Test(groups = {"arenadata"}, description = "Check not encrypted password")
    public void checkNotEncryptedPassword() throws Exception {
        copyAndModifyJdbcConfFile(PASSWORD);
        runSqlTest("arenadata/encryption/success-encrypted");
    }

    private void copyAndModifyJdbcConfFile(String password) throws Exception {
        cluster.deleteFileFromNodes(pxfJdbcSiteConfFile, false);
        cluster.copyFileToNodes(pxfJdbcSiteConfTemplate, pxfJdbcSiteConfPath, true, false);
        cluster.runCommandOnAllNodes("sed -i 's/JDBC_PASSWORD/" + password + "/' " + pxfJdbcSiteConfFile);
    }
}
