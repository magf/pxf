package org.greenplum.pxf.automation.features.cloud;

import annotations.WorksWithFDW;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.minio.Minio;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.nio.file.Paths;

@WorksWithFDW
@Feature("S3 Cloud Access")
public class CloudAccessTest extends BaseFeature {

    private static final String[] PXF_MULTISERVER_COLS = {
            "name text",
            "num integer",
            "dub double precision",
            "longNum bigint",
            "bool boolean"
    };

    private static final String[] PXF_WRITE_COLS = {
            "name text",
            "score integer"
    };

    private static final String BUCKET_NAME = "pxf-s3";

    private Minio minio;

    /**
     * Prepare all server configurations and components
     */
    @Override
    public void beforeClass() throws Exception {
        minio = (Minio) SystemManagerImpl.getInstance().getSystemObject("minio");
        cleanBucket();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        cleanBucket();
    }

    /*
     * The tests below are for the case where there's NO Hadoop cluster configured under "default" server
     * and assumes the "default" server has not configuration files. They are part of "s3" group and do not
     * make sense in the environment with Kerberized Hadoop, where the tests in the "security" group would run
     */

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenNoServerNoCredsSpecified() throws Exception {
        runTestScenario("no_server_no_credentials", null, false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenServerNoCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_no_config", "s3-non-existent", false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenNoServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("no_server_credentials_no_config", null, true);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessFailsWhenServerNoCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_invalid_config", "s3-invalid", false);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenServerCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_credentials_invalid_config", "s3-invalid", true);
    }

    @Test(groups = {"s3"})
    public void testCloudAccessOkWhenServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_credentials_no_config", "s3-non-existent", true);
    }

    /*
     * The tests below are for the case where there's a Hadoop cluster configured under "default" server
     * both without and with Kerberos security, testing that cloud access works in presence of "default" server
     */

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsFailsWhenNoServerNoCredsSpecified() throws Exception {
        runTestScenario("no_server_no_credentials_with_hdfs", null, false);
    }

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsOkWhenServerNoCredsValidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_valid_config_with_hdfs", "s3", false);
    }

    @Test(groups = {"security", "gpdb"})
    public void testCloudWriteWithHdfsOkWhenServerNoCredsValidConfigFileExists() throws Exception {
        runTestScenarioForWrite("server_no_credentials_valid_config_with_hdfs_write", "s3", false);
    }

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsFailsWhenServerNoCredsNoConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_no_config_with_hdfs", "s3-non-existent", false);
    }

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsFailsWhenNoServerCredsNoConfigFileExists() throws Exception {
        runTestScenario("no_server_credentials_no_config_with_hdfs", null, true);
    }

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsFailsWhenServerNoCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_no_credentials_invalid_config_with_hdfs", "s3-invalid", false);
    }

    @Test(groups = {"security"})
    public void testCloudAccessWithHdfsOkWhenServerCredsInvalidConfigFileExists() throws Exception {
        runTestScenario("server_credentials_invalid_config_with_hdfs", "s3-invalid", true);
    }

    private void runTestScenario(String name, String server, boolean creds) throws Exception {
        String tableName = "cloudaccess_" + name;
        String locationPath =  BUCKET_NAME + "/" + tableName + "/" + fileName;

        minio.createBucket(BUCKET_NAME);
        minio.uploadFile(BUCKET_NAME, tableName + "/" + fileName, Paths.get(localDataResourcesFolder + "/cloud/" + fileName));

        exTable = TableFactory.getPxfReadableTextTable(tableName, PXF_MULTISERVER_COLS, locationPath, ",");
        exTable.setProfile("s3:text");
        String serverParam = (server == null) ? null : "server=" + server;
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + minio.getAccessKeyId(), "secretkey=" + minio.getSecretKey()});
        }
        gpdb.createTableAndVerify(exTable);

        runSqlTest("features/cloud_access/" + name);
    }

    private void runTestScenarioForWrite(String name, String server, boolean creds) throws Exception {
        // create writable external table to write to S3
        String tableName = "cloudwrite_" + name;
        String locationPath = BUCKET_NAME + "/" + tableName;
        minio.createBucket(BUCKET_NAME);

        exTable = TableFactory.getPxfWritableTextTable(tableName, PXF_WRITE_COLS, locationPath, ",");
        exTable.setProfile("s3:text");
        String serverParam = (server == null) ? null : "server=" + server;
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + minio.getAccessKeyId(), "secretkey=" + minio.getSecretKey()});
        }
        gpdb.createTableAndVerify(exTable);

        // create readable external table to read back from S3, making sure previous insert made it all the way to S3
        tableName = "cloudaccess_" + name;
        exTable = TableFactory.getPxfReadableTextTable(tableName, PXF_WRITE_COLS, locationPath, ",");
        exTable.setProfile("s3:text");
        exTable.setServer(serverParam);
        if (creds) {
            exTable.setUserParameters(new String[]{"accesskey=" + ProtocolUtils.getAccess(), "secretkey=" + ProtocolUtils.getSecret()});
        }
        gpdb.createTableAndVerify(exTable);

        runSqlTest("features/cloud_access/" + name);
    }

    private void cleanBucket() {
        if (minio != null) {
            minio.clean(BUCKET_NAME);
        }
    }
}
