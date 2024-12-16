package org.greenplum.pxf.automation.features.cloud;

import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.minio.Minio;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.pxf.WritableExternalTable;
import org.greenplum.pxf.automation.utils.tables.ComparisonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;

import static org.greenplum.pxf.automation.features.tpch.LineItem.LINEITEM_SCHEMA;

@Feature("S3 Minio")
public class S3Test extends BaseFeature {

    private static final String BUCKET_NAME = "pxf-s3";
    private static final String S3_SOURCE_TABLE_NAME = "s3_source_table";

    private Minio minio;
    private Table s3SourceTable;

    @Override
    public void beforeClass() throws Exception {
        minio = (Minio) SystemManagerImpl.getInstance().getSystemObject("minio");
        cleanBucket();
        prepareS3SourceTable();
    }

    @Override
    protected void afterClass() throws Exception {
        super.afterClass();
        cleanBucket();
        gpdb.dropTable(s3SourceTable, true);
    }

    @Test(groups = {"s3", "gpdb"},
            dataProvider = "s3DataTypeConfigurations")
    public void runS3Test(String name, String profile, boolean setFormatter, String format, String delimiter, String[] userParameters) throws Exception {
        // create writable external table to write to S3
        String tableName = "s3_write_" + name;
        String locationPath = BUCKET_NAME + "/" + tableName;
        minio.createBucket(BUCKET_NAME);

        WritableExternalTable writableExternalTable = new WritableExternalTable(tableName, LINEITEM_SCHEMA, locationPath, format);
        writableExternalTable.setProfile(profile);
        writableExternalTable.setServer("server=s3");
        if (setFormatter) {
            writableExternalTable.setFormatter("pxfwritable_export");
        }
        if (delimiter != null)
            writableExternalTable.setDelimiter(delimiter);
        if (userParameters != null)
            writableExternalTable.setUserParameters(userParameters);

        gpdb.createTableAndVerify(writableExternalTable);
        gpdb.insertData(s3SourceTable, writableExternalTable);

        // create readable external table to read back from S3, making sure previous insert made it all the way to S3
        tableName = "s3_read_" + name;

        ReadableExternalTable readableExTable = new ReadableExternalTable(tableName, LINEITEM_SCHEMA, locationPath, format);
        readableExTable.setProfile(profile);
        readableExTable.setServer("server=s3");
        if (setFormatter) {
            readableExTable.setFormatter("pxfwritable_import");
        }
        if (delimiter != null)
            readableExTable.setDelimiter(delimiter);
        if (userParameters != null)
            readableExTable.setUserParameters(userParameters);
        gpdb.createTableAndVerify(readableExTable);

        gpdb.queryResults(readableExTable, "SELECT * FROM " + readableExTable.getName() + " ORDER BY 1");
        ComparisonUtils.compareTables(s3SourceTable, readableExTable, null);
    }

    @Step("Prepare s3 source table")
    private void prepareS3SourceTable() throws Exception {
        s3SourceTable = new Table(S3_SOURCE_TABLE_NAME, LINEITEM_SCHEMA);
        s3SourceTable.setDistributionFields(new String[]{"l_orderkey"});
        gpdb.createTableAndVerify(s3SourceTable);
        gpdb.copyFromFile(s3SourceTable, new File(localDataResourcesFolder
                + "/s3/" + "sample.csv"), "E'|'", "E'\\\\N'", true);
        gpdb.queryResults(s3SourceTable, "SELECT * FROM " + s3SourceTable.getName() + " ORDER BY 1");
    }

    private void cleanBucket() {
        if (minio != null) {
            minio.clean(BUCKET_NAME);
        }
    }

    @DataProvider(name = "s3DataTypeConfigurations")
    private static Object[][] s3DataFileTypes() {
        return new Object[][]{
                {"csv", "s3:csv", false, "CSV", "|", null},
                {"csv_gzip", "s3:csv", false, "CSV", "|", new String[]{"COMPRESSION_CODEC=gzip"}},
                {"csv_bzip2", "s3:csv", false, "CSV", "|", new String[]{"COMPRESSION_CODEC=bzip2"}},
                {"text", "s3:text", false, "TEXT", "|", null},
                {"text_gzip", "s3:text", false, "TEXT", "|", new String[]{"COMPRESSION_CODEC=gzip"}},
                {"text_bzip2", "s3:text", false, "TEXT", "|", new String[]{"COMPRESSION_CODEC=bzip2"}},
                {"parquet", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=uncompressed"}},
                {"parquet_snappy", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=snappy"}},
                {"parquet_gzip", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=gzip"}},
                {"parquet_lz4", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=lz4"}},
                {"parquet_zstd", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=zstd"}},
                {"parquet_lz4_raw", "s3:parquet", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=lz4_raw"}},
                {"orc", "s3:orc", true, "CUSTOM", null, null},
                {"orc_snappy", "s3:orc", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=snappy"}},
                {"orc_zlib", "s3:orc", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=zlib"}},
                {"orc_lz4", "s3:orc", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=lz4"}},
                {"orc_zstd", "s3:orc", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=zstd"}},
                {"avro", "s3:avro", true, "CUSTOM", null, null},
                {"avro_deflate", "s3:avro", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=deflate"}},
                {"avro_xz", "s3:avro", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=xz"}},
                {"avro_bzip2", "s3:avro", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=bzip2"}},
                {"json", "s3:json", true, "CUSTOM", null, null},
                {"json_bzip", "s3:json", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=bzip2"}},
                {"json_gzip", "s3:json", true, "CUSTOM", null, new String[]{"COMPRESSION_CODEC=gzip"}},
        };
    }
}
