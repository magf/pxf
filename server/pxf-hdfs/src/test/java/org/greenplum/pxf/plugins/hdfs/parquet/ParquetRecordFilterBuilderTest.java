package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.plugins.hdfs.utilities.DecimalOverflowOption;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetRecordFilterBuilderTest extends ParquetBaseTest {

    @Test
    public void testUnsupportedOperationError() {
        // a16 in (11, 12)
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a16m1007s2d11s2d12o10"));
        assertEquals("not supported IN", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96EqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o5"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96LessThanFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o1"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96GreaterThanFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o2"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96LessThanOrEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o3"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96GreaterThanOrEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o4"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96NotEqualsFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6c1114s19d2013-07-23 21:00:00o6"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96IsNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6o8"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testUnsupportedINT96IsNotNullFilter() {
        // tm = '2013-07-23 21:00:00'
        Exception e = assertThrows(UnsupportedOperationException.class,
                () -> filterBuilderFromFilterString("a6o9"));
        assertEquals("Column tm of type INT96 is not supported", e.getMessage());
    }

    @Test
    public void testSupportedFixedLenByteArrayEqualsFilter() throws Exception {
        // dec2 = 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o5");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("eq(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayLessThanFilter() throws Exception {
        // dec2 < 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o1");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("lt(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayGreaterThanFilter() throws Exception {
        // dec2 > 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o2");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("gt(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayLessThanOrEqualsFilter() throws Exception {
        // dec2 <= 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o3");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("lteq(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayGreaterThanOrEqualsFilter() throws Exception {
        // dec2 >= 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o4");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("gteq(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayNotEqualsFilter() throws Exception {
        // dec2 != 0
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14c23s1d0o6");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("noteq(dec2, Binary{3 reused bytes, [0, 0, 0]})", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayIsNullFilter() throws Exception {
        // dec2 == null
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14o8");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("eq(dec2, null)", filterPredicate.toString());
    }

    @Test
    public void testSupportedFixedLenByteArrayIsNotNullFilter() throws Exception {
        // dec2 != null
        ParquetRecordFilterBuilder filterBuilder = filterBuilderFromFilterString("a14o9");
        FilterPredicate filterPredicate = getFilterPredicate(filterBuilder);
        assertEquals("noteq(dec2, null)", filterPredicate.toString());
    }

    private static FilterPredicate getFilterPredicate(ParquetRecordFilterBuilder filterBuilder) {
        assertNotNull(filterBuilder);
        FilterCompat.Filter recordFilter = filterBuilder.getRecordFilter();
        assertNotNull(recordFilter);
        assertInstanceOf(FilterCompat.FilterPredicateCompat.class, recordFilter);
        FilterPredicate filterPredicate = ((FilterCompat.FilterPredicateCompat) recordFilter).getFilterPredicate();
        return filterPredicate;
    }

    private ParquetRecordFilterBuilder filterBuilderFromFilterString(String filterString) throws Exception {

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(columnDescriptors, originalFieldsMap,
                DecimalOverflowOption.IGNORE, true);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // traverse the tree with the ParquetRecordFilterBuilder to
        // produce a record filter for parquet
        TRAVERSER.traverse(root, filterBuilder);

        return filterBuilder;
    }
}
