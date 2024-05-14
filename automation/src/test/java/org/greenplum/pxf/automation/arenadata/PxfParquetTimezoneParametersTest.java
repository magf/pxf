package org.greenplum.pxf.automation.arenadata;

import annotations.WorksWithFDW;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.util.List;

import static org.greenplum.pxf.automation.components.cluster.PhdCluster.EnumClusterServices.pxf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

@WorksWithFDW
public class PxfParquetTimezoneParametersTest extends BaseFeature {
    private static final String PARQUET_FORMAT = "parquet";
    private static final String WRITABLE_EXTERNAL_TABLE_NAME = "writable_parquet_table";
    private static final String READABLE_EXTERNAL_TABLE_NAME = "readable_parquet_table";
    private static final String USE_INT64_TIMESTAMPS_PARAM = "USE_INT64_TIMESTAMPS=";
    private static final String USE_LOCAL_PXF_TIMEZONE_WRITE_PARAM = "USE_LOCAL_PXF_TIMEZONE_WRITE=";
    private static final String USE_LOCAL_PXF_TIMEZONE_READ_PARAM = "USE_LOCAL_PXF_TIMEZONE_READ=";
    private static final String PXF_ENV_FILE_RELATIVE_PATH = "conf/pxf-env.sh";
    private static final String[] TIMESTAMP = new String[]{"('2022-06-22 19:10:25')"};
    private static final String[] TIMESTAMP_WITH_TIMEZONE = new String[]{"('2022-06-22 19:10:25.123456+04')"};
    private static final String SCHEMA = "tmp        TIMESTAMP";
    private static final String SCHEMA_WITH_TIMEZONE = "tmp        TIMESTAMP WITH TIME ZONE";
    private static final String TMP_PARQUET_FILE_NAME = "tmp_no_timezone.parquet";
    private static final String TMP_WITH_TIMEZONE_PARQUET_FILE_NAME = "tmp_with_timezone.parquet";
    private String hdfsPath;

    @Override
    protected void beforeClass() throws Exception {
        hdfsPath = hdfs.getWorkingDirectory() + "/parquet/";
        String pxfEnvFile = cluster.getPxfHome() + "/" + PXF_ENV_FILE_RELATIVE_PATH;
        cluster.runCommandOnAllNodes("sed -i 's|# export PXF_JVM_OPTS=\"-Xmx2g -Xms1g\"|export PXF_JVM_OPTS=\"-Xmx2g -Xms1g -Duser.timezone=\"Europe/Moscow\"\"|' " + pxfEnvFile);
        cluster.restart(pxf);
    }


    @Test(groups = {"arenadata"}, dataProvider = "useInt64TimestampsProvider")
    public void testUseInt64Timestamps(String[] tableSchema, String timestampValue, boolean useInt64IsEnabled, PrimitiveType.PrimitiveTypeName primitiveTypeName) throws Exception {
        String useInt64Param = USE_INT64_TIMESTAMPS_PARAM + useInt64IsEnabled;
        exTable = TableFactory.getPxfHcfsWritableTable(WRITABLE_EXTERNAL_TABLE_NAME, tableSchema, hdfsPath, hdfs.getBasePath(), PARQUET_FORMAT);
        exTable.setUserParameters(new String[]{useInt64Param});
        createTable(exTable);
        gpdb.insertData(timestampValue, exTable);
        Path parquetPath = new Path(hdfs.list(hdfsPath).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No parquet file is found in this path")));
        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(
                hdfs.getConfiguration(), parquetPath, ParquetMetadataConverter.NO_FILTER);
        MessageType parquetData = parquetMetadata.getFileMetaData().getSchema();
        List<Type> parquetSchema = parquetData.getFields();
        assertEquals(1, parquetSchema.size());
        Type tmpType = parquetSchema.get(0);
        assertEquals("tmp", tmpType.getName());
        assertTrue(tmpType.isPrimitive());
        assertEquals(primitiveTypeName, ((PrimitiveType) tmpType).getPrimitiveTypeName());
    }

    @Test(groups = {"arenadata"}, dataProvider = "useLocalPxfTimezoneWriteProvider")
    public void testUseLocalPxfTimezoneWrite(String[] tableSchema, String timestampValue, boolean useLocalPxfTimezoneWriteIsEnabled, String sqlPath) throws Exception {
        String fullTestPath = hdfsPath + "use_local_pxf_timezone_write";
        String useLocalPxfTimezoneWrite = USE_LOCAL_PXF_TIMEZONE_WRITE_PARAM + useLocalPxfTimezoneWriteIsEnabled;
        exTable = TableFactory.getPxfHcfsWritableTable(WRITABLE_EXTERNAL_TABLE_NAME, tableSchema, fullTestPath, hdfs.getBasePath(), PARQUET_FORMAT);
        exTable.setUserParameters(new String[]{useLocalPxfTimezoneWrite});
        createTable(exTable);
        gpdb.insertData(timestampValue, exTable);
        String useLocalPxfTimezoneRead = USE_LOCAL_PXF_TIMEZONE_READ_PARAM + false;
        exTable = TableFactory.getPxfHcfsReadableTable(READABLE_EXTERNAL_TABLE_NAME, tableSchema, fullTestPath, hdfs.getBasePath(), PARQUET_FORMAT);
        exTable.setUserParameters(new String[]{useLocalPxfTimezoneRead});
        createTable(exTable);
        runSqlTest(sqlPath);
    }

    @Test(groups = {"arenadata"}, dataProvider = "useLocalPxfTimezoneReadProvider")
    public void testUseLocalPxfTimezoneRead(String[] tableSchema, String resourceFile, boolean useLocalPxfTimezoneReadIsEnabled, String sqlPath) throws Exception {
        String resourcePath = localDataResourcesFolder + "/parquet/";
        hdfs.copyFromLocal(resourcePath + resourceFile, hdfsPath);
        String useLocalPxfTimezoneRead = USE_LOCAL_PXF_TIMEZONE_READ_PARAM + useLocalPxfTimezoneReadIsEnabled;
        exTable = TableFactory.getPxfHcfsReadableTable(READABLE_EXTERNAL_TABLE_NAME, tableSchema, hdfsPath, hdfs.getBasePath(), PARQUET_FORMAT);
        exTable.setUserParameters(new String[]{useLocalPxfTimezoneRead});
        createTable(exTable);
        runSqlTest(sqlPath);
    }

    @Override
    protected void afterMethod() throws Exception {
        super.afterMethod();
        hdfs.removeDirectory(hdfsPath);
    }

    @DataProvider
    private Object[][] useInt64TimestampsProvider() {
        return new Object[][]{
                {SCHEMA, TIMESTAMP, true, INT64},
                {SCHEMA_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, true, INT64},
                {SCHEMA, TIMESTAMP, false, INT96},
                {SCHEMA_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, false, INT96},
        };
    }

    @DataProvider
    private Object[][] useLocalPxfTimezoneWriteProvider() {
        return new Object[][]{
                {SCHEMA, TIMESTAMP, true, "arenadata/parquet/check-writable-tables/timestamp/true"},
                {SCHEMA_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, true, "arenadata/parquet/check-writable-tables/timestamp-with-timezone"},
                {SCHEMA, TIMESTAMP, false, "arenadata/parquet/check-writable-tables/timestamp/false"},
                {SCHEMA_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, false, "arenadata/parquet/check-writable-tables/timestamp-with-timezone"},
        };
    }

    @DataProvider
    private Object[][] useLocalPxfTimezoneReadProvider() {
        return new Object[][]{
                {SCHEMA, TMP_PARQUET_FILE_NAME, true, "arenadata/parquet/check-readable-tables/timestamp/true"},
                {SCHEMA_WITH_TIMEZONE, TMP_WITH_TIMEZONE_PARQUET_FILE_NAME, true, "arenadata/parquet/check-readable-tables/timestamp-with-timezone/true"},
                {SCHEMA, TMP_PARQUET_FILE_NAME, false, "arenadata/parquet/check-readable-tables/timestamp/false"},
                {SCHEMA_WITH_TIMEZONE, TMP_WITH_TIMEZONE_PARQUET_FILE_NAME, false, "arenadata/parquet/check-readable-tables/timestamp-with-timezone/false"}
        };
    }

}
