package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.greenplum.pxf.plugins.hdfs.ParquetResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGInterval;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for filtering extended types (time, interval etc)
 * Memo for comparison operators in filters
 * <attcode><attnum><constcode><constval><constsizecode><constsize><constdata><constvalue><opercode><opernum>
 * <p>
 * Example filter list:
 * <p>
 * Column(0) > 1 AND Column(0) < 5 AND Column(2) == "third"
 * <p>
 * Yields the following serialized string:
 * <p>
 * a0c23s1d1o2a1c23s1d5o1a2c25s5dthirdo5l0l0
 * <p>
 * Where:
 * <p>
 * a0	  - first column of table
 * c23	  - scalar constant with type oid 23(INT4)
 * s1	  - size of constant in bytes
 * d1	  - serialized constant value
 * o2	  - greater than operation
 * a1	  - second column of table
 * c23	  - scalar constant with type oid 23(INT4)
 * s1	  - size of constant in bytes
 * d5	  - serialized constant value
 * o1	  - less than operation
 * a2	  - third column of table
 * c25	  - scalar constant with type oid 25(TEXT)
 * s5	  - size of constant in bytes
 * dthird - serialized constant value
 * o5	  - equals operation
 * l0	  - AND operator
 * l0	  - AND operator
 * o0 - noop (translates to == still)
 * o1 - <
 * o2 - >
 * o3 - <=
 * o4 - >=
 * o5 - ==
 * o6 - !=
 * o7 - like
 * o8 - is null
 * o9 - is not null
 * o10 - in
 */
public class ParquetExtendedFilterPushDownTest extends ParquetBaseTest {

    // From resources/parquet/extended_types.parquet
    private static final int[] ID_COL = {1, 2, 3, 4, 5};
    private static final String[] TIME_COL = {"13:13:00", "18:54:00", "23:15:00", null, "19:35:00"};
    private static final String[] INTERVAL_COL = {"3 month 4 day 5 hour 6 minute 7 second", "2 year 3 month 4 day 5 hour 6 minute 7 second", "4 day 5 hour 6 minute 7 second", "11 second", null};
    private static final String[] UUID_COL = {"666b7805-a0f3-4ac0-90d7-4fc33ef9e25a", "4350f1fa-dead-beef-ba5d-b84af90d7954", "6d1903b4-d64d-47b1-949b-003307ad84f4", "dead03b4-dead-47b1-beef-003307adbeef", null};
    private static final String[] JSON_COL = {"{\"a\": 1, \"a\": 2}", "{\"a\": 4, \"a\": 5}", "{\"a\": 6, \"a\": 7}", null, "{\"a\": 12, \"a\": 13}"};
    private static final String[] BSON_COL = {"{\"a\": 1}", "{\"a\": 3}", "{\"a\": 11}", "{\"a\": 11}", null};
    private static final String[] DECIMAL_COL = {"666666.66", null, "12345.67", "333777.99", "1000000000.00"};
    private static final int[] ALL = ID_COL;

    private Accessor accessor;
    private Resolver resolver;
    private RequestContext context;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();

        accessor = new ParquetFileAccessor();
        resolver = new ParquetResolver();
        context = new RequestContext();

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("parquet/extended_types.parquet")).getPath();

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setProfileScheme("localfile");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setDataSource(path);
        context.setFragmentMetadata(new HcfsFragmentMetadata(0, 4196));
        context.setTupleDescription(super.extendedColumnDescriptors);
        context.setConfiguration(new Configuration());

        accessor.setRequestContext(context);
        resolver.setRequestContext(context);
        accessor.afterPropertiesSet();
        resolver.afterPropertiesSet();
    }

    @Test
    public void testNoFilter() throws Exception {
        // all rows are expected
        assertRowsReturned(ALL);
    }

    @Test
    public void testTimePushDown() throws Exception {
        int[] expectedRows = {1};
        // a1 == 13:13
        context.setFilterString("a1c1083s8d13:13:00o5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3, 5};
        // a1 > 13:13
        context.setFilterString("a1c1083s8d13:13:00o2");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 5};
        // a1 < 23:15
        context.setFilterString("a1c1083s8d23:15:00o1");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3, 4, 5};
        // a1 <> 13:13
        context.setFilterString("a1c1083s8d13:13:00o6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3, 5};
        // a1 >= 18:54
        context.setFilterString("a1c1083s8d18:54:00o4");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2};
        // a1 <= 18:54
        context.setFilterString("a1c1083s8d18:54:00o3");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {4};
        // a1 is null
        context.setFilterString("a1o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 5};
        // a1 is not null
        context.setFilterString("a1o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2};
        // a1 in (18:54, 13:13)
        context.setFilterString("a1m1183s8d18:54:00s8d13:13:00o10");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testIntervalPushDown() throws Exception {
        int[] expectedRows = {1};
        // a2 == 3 month 4 day 5 hour 6 minute 7 second
        context.setFilterString("a2c1186s38d3 month 4 day 5 hour 6 minute 7 secondo5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2};
        // a2 > 3 month 4 day 5 hour 6 minute 7 second
        context.setFilterString("a2c1186s38d3 month 4 day 5 hour 6 minute 7 secondo2");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 3, 4};
        // a2 < 2 year 3 month 4 day 5 hour 6 minute 7 second
        context.setFilterString("a2c1186s45d2 year 3 month 4 day 5 hour 6 minute 7 secondo1");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 4, 5};
        // a2 <> 4 day 5 hour 6 minute 7 seconds
        context.setFilterString("a2c1186s31d4 day 5 hour 6 minute 7 secondso6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3};
        // a2 >= 4 day 5 hour 6 minute 7 seconds
        context.setFilterString("a2c1186s31d4 day 5 hour 6 minute 7 secondso4");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 4};
        // a2 <= 2 year 3 month 4 day 5 hour 6 minute 7 second
        context.setFilterString("a2c1186s45d2 year 3 month 4 day 5 hour 6 minute 7 secondo3");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {5};
        // a2 is null
        context.setFilterString("a2o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 4};
        // a2 is not null
        context.setFilterString("a2o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 3};
        // a2 in (4 day 5 hour 6 minute 7 second, 3 month 4 day 5 hour 6 minute 7 second)
        context.setFilterString("a2m1187s30d4 day 5 hour 6 minute 7 seconds38d3 month 4 day 5 hour 6 minute 7 secondo10");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testUUIDPushDown() throws Exception {
        int[] expectedRows = {1};
        // a3 == 666b7805-a0f3-4ac0-90d7-4fc33ef9e25a
        context.setFilterString("a3c2950s36d666b7805-a0f3-4ac0-90d7-4fc33ef9e25ao5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3, 4, 5};
        // a3 <> 666b7805-a0f3-4ac0-90d7-4fc33ef9e25a
        context.setFilterString("a3c2950s36d666b7805-a0f3-4ac0-90d7-4fc33ef9e25ao6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {5};
        // a3 is null
        context.setFilterString("a3o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 4};
        // a3 is not null
        context.setFilterString("a3o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 3};
        // a3 in (6d1903b4-d64d-47b1-949b-003307ad84f4, 666b7805-a0f3-4ac0-90d7-4fc33ef9e25a)
        context.setFilterString("a3m2951s36d666b7805-a0f3-4ac0-90d7-4fc33ef9e25as36d6d1903b4-d64d-47b1-949b-003307ad84f4o10");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testJsonPushDown() throws Exception {
        int[] expectedRows = {1};
        // a4 == {"a": 1, "a": 2}
        context.setFilterString("a4c114s16d{\"a\": 1, \"a\": 2}o5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3, 4, 5};
        // a4 <> {"a": 1, "a": 2}
        context.setFilterString("a4c114s16d{\"a\": 1, \"a\": 2}o6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {4};
        // a4 is null
        context.setFilterString("a4o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 5};
        // a4 is not null
        context.setFilterString("a4o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2, 3};
        // a4 in ({"a": 4, "a": 5}, {"a": 6, "a": 7})
        context.setFilterString("a4m199s16d{\"a\": 4, \"a\": 5}s16d{\"a\": 6, \"a\": 7}o10");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testBsonPushDown() throws Exception {
        int[] expectedRows = {3, 4};
        // a5 == {"a": 11}
        context.setFilterString("a5c3802s9d{\"a\": 11}o5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 5};
        // a5 <> {"a": 11}
        context.setFilterString("a5c3802s9d{\"a\": 11}o6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {5};
        // a5 is null
        context.setFilterString("a5o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 3, 4};
        // a5 is not null
        context.setFilterString("a5o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2};
        // a5 in ({"a": 1}, {"a": 3})
        context.setFilterString("a5m3807s8d{\"a\": 1}s8d{\"a\": 3}o10");
        assertRowsReturned(expectedRows);
    }

    @Test
    public void testDecimalPushDown() throws Exception {
        int[] expectedRows = {1};
        // a6 == 666666.66
        context.setFilterString("a6c1700s9d666666.66o5");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 5};
        // a6 > 333777.99
        context.setFilterString("a6c1700s9d333777.99o2");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {3};
        // a6 < 333777.99
        context.setFilterString("a6c1700s9d333777.99o1");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 2, 4, 5};
        // a6 <> 12345.67
        context.setFilterString("a6c1700s8d12345.67o6");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 4, 5};
        // a6 >= 333777.99
        context.setFilterString("a6c1700s9d333777.99o4");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 3, 4};
        // a6 <= 666666.66
        context.setFilterString("a6c1700s9d666666.66o3");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {2};
        // a6 is null
        context.setFilterString("a6o8");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 3, 4, 5};
        // a6 is not null
        context.setFilterString("a6o9");
        assertRowsReturned(expectedRows);

        expectedRows = new int[] {1, 5};
        // a6 in (1000000000.00, 666666.66)
        context.setFilterString("a6m1231s13d1000000000.00s9d666666.66o10");
        assertRowsReturned(expectedRows);
    }
    
    private void assertRowsReturned(int[] expectedRows) throws Exception {
        assertTrue(accessor.openForRead());

        OneRow oneRow;
        for (int expectedRow : expectedRows) {
            oneRow = accessor.readNextObject();
            assertNotNull(oneRow, "Row " + expectedRow);
            List<OneField> fieldList = resolver.getFields(oneRow);
            assertNotNull(fieldList, "Row " + expectedRow);
            assertEquals(7, fieldList.size(), "Row " + expectedRow);

            assertTypes(fieldList);
            assertValues(fieldList, expectedRow - 1);
        }
        oneRow = accessor.readNextObject();
        assertNull(oneRow, "No more rows expected");

        accessor.closeForRead();
    }

    private void assertTypes(List<OneField> fieldList) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(DataType.INTEGER.getOID(), fieldList.get(0).type);
        }
        if (columnDescriptors.get(1).isProjected()) {
            assertEquals(DataType.TIME.getOID(), fieldList.get(1).type);
        }
        if (columnDescriptors.get(2).isProjected()) {
            assertEquals(DataType.INTERVAL.getOID(), fieldList.get(2).type);
        }
        if (columnDescriptors.get(3).isProjected()) {
            assertEquals(DataType.UUID.getOID(), fieldList.get(3).type);
        }
        if (columnDescriptors.get(4).isProjected()) {
            assertEquals(DataType.JSON.getOID(), fieldList.get(4).type);
        }
        if (columnDescriptors.get(5).isProjected()) {
            assertEquals(DataType.JSONB.getOID(), fieldList.get(5).type);
        }
        if (columnDescriptors.get(6).isProjected()) {
            assertEquals(DataType.NUMERIC.getOID(), fieldList.get(6).type);
        }
    }

    private void assertValues(List<OneField> fieldList, final int row) throws SQLException {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();

        if (columnDescriptors.get(0).isProjected()) {
            assertEquals(ID_COL[row], fieldList.get(0).val, "Row " + row);
        } else {
            assertNull(fieldList.get(0).val, "Row " + row);
        }

        if (columnDescriptors.get(1).isProjected()) {
            assertEquals(TIME_COL[row], fieldList.get(1).val, "Row " + row);
        } else {
            assertNull(fieldList.get(1).val, "Row " + row);
        }

        if (columnDescriptors.get(2).isProjected() && INTERVAL_COL[row] != null) {
            assertEquals(new PGInterval(INTERVAL_COL[row]).toString(), fieldList.get(2).val, "Row " + row);
        } else {
            assertNull(fieldList.get(2).val, "Row " + row);
        }

        if (columnDescriptors.get(3).isProjected() && UUID_COL[row] != null) {
            assertEquals(UUID_COL[row], fieldList.get(3).val, "Row " + row);
        } else {
            assertNull(fieldList.get(3).val, "Row " + row);
        }

        if (columnDescriptors.get(4).isProjected() && JSON_COL[row] != null) {
            assertEquals(JSON_COL[row], fieldList.get(4).val, "Row " + row);
        } else {
            assertNull(fieldList.get(4).val, "Row " + row);
        }

        if (columnDescriptors.get(5).isProjected() && BSON_COL[row] != null) {
            assertEquals(BSON_COL[row], fieldList.get(5).val, "Row " + row);
        } else {
            assertNull(fieldList.get(5).val, "Row " + row);
        }

        if (columnDescriptors.get(6).isProjected() && DECIMAL_COL[row] != null) {
            assertEquals(new BigDecimal(DECIMAL_COL[row]), fieldList.get(6).val, "Row " + row);
        } else {
            assertNull(fieldList.get(6).val, "Row " + row);
        }
    }

}
