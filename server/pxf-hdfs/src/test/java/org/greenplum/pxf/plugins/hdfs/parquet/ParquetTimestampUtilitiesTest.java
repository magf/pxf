package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.Int64ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.parquet.converters.ParquetTypeConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParquetTimestampUtilitiesTest {
    @Mock
    private Group group;
    @Mock
    private Type type;
    @Mock
    private LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation;
    private final int columnIndex = 1;
    private final int repeatIndex = 1;

    @Test
    void getDaysFromEpochFromDateString() {
        // Epoch days: 2901
        String day = "1977-12-11";
        int epochDays = ParquetTimestampUtilities.getDaysFromEpochFromDateString(day);
        assertEquals(2901, epochDays);
    }

    @Test
    public void testStringConversionRoundTrip() {
        String timestamp = "2019-03-14 20:52:48.123456";
        Binary binary = ParquetTimestampUtilities.getBinaryFromTimestamp(timestamp);
        String convertedTimestamp = ParquetTimestampUtilities.bytesToTimestamp(binary.getBytes());

        assertEquals(timestamp, convertedTimestamp);
    }

    @Test
    public void testBinaryConversionRoundTrip() {
        // 2019-03-14 21:22:05.987654
        byte[] source = new byte[]{112, 105, -24, 125, 77, 14, 0, 0, -66, -125, 37, 0};
        String timestamp = ParquetTimestampUtilities.bytesToTimestamp(source);
        Binary binary = ParquetTimestampUtilities.getBinaryFromTimestamp(timestamp);

        assertArrayEquals(source, binary.getBytes());
    }

    @Test
    public void testUnsupportedNanoSeconds() {
        String timestamp = "2019-03-14 20:52:48.1234567";
        Exception e = assertThrows(DateTimeParseException.class,
                () -> ParquetTimestampUtilities.getBinaryFromTimestamp(timestamp));
        assertEquals("Text '2019-03-14 20:52:48.1234567' could not be parsed, unparsed text found at index 26", e.getMessage());
    }

    @Test
    public void testBinaryWithNanos() {
        Instant instant = Instant.parse("2019-03-15T03:52:48.123456Z"); // UTC
        ZonedDateTime localTime = instant.atZone(ZoneId.systemDefault());
        String expected = localTime.format(GreenplumDateTime.DATETIME_FORMATTER); // should be "2019-03-14 20:52:48.123456" in PST

        byte[] source = new byte[]{0, 106, 9, 53, -76, 12, 0, 0, -66, -125, 37, 0}; // represents 2019-03-14 20:52:48.1234567
        String timestamp = ParquetTimestampUtilities.bytesToTimestamp(source); // nanos get dropped
        assertEquals(expected, timestamp);
    }

    @Test
    public void testTimestampWithTimezoneStringConversionRoundTrip() {
        String expectedTimestampInUTC = "2016-06-22 02:06:25";
        String expectedTimestampInSystemTimeZone = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC);

        // Conversion roundtrip for test input (timestamp)
        String timestamp = "2016-06-21 22:06:25-04";
        Binary binary = ParquetTimestampUtilities.getBinaryFromTimestampWithTimeZone(timestamp);
        String convertedTimestamp = ParquetTimestampUtilities.bytesToTimestamp(binary.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone, convertedTimestamp);
    }

    @Test
    public void testTimestampWithTimezoneWithMicrosecondsStringConversionRoundTrip() {
        // Case 1
        String expectedTimestampInUTC = "2019-07-11 01:54:53.523485";
        // We're using expectedTimestampInSystemTimeZone as expected string for testing as the timestamp is expected to be converted to system's local time
        String expectedTimestampInSystemTimeZone = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC);

        // Conversion roundtrip for test input (timestamp); (test input will lose time zone information but remain correct value, and test against expectedTimestampInSystemTimeZone)
        String timestamp = "2019-07-10 21:54:53.523485-04";
        Binary binary = ParquetTimestampUtilities.getBinaryFromTimestampWithTimeZone(timestamp);
        String convertedTimestamp = ParquetTimestampUtilities.bytesToTimestamp(binary.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone, convertedTimestamp);

        // Case 2
        String expectedTimestampInUTC2 = "2019-07-10 18:54:47.354795";
        String expectedTimestampInSystemTimeZone2 = convertUTCToCurrentSystemTimeZone(expectedTimestampInUTC2);

        // Conversion roundtrip for test input (timestamp)
        String timestamp2 = "2019-07-11 07:39:47.354795+12:45";
        Binary binary2 = ParquetTimestampUtilities.getBinaryFromTimestampWithTimeZone(timestamp2);
        String convertedTimestamp2 = ParquetTimestampUtilities.bytesToTimestamp(binary2.getBytes());

        assertEquals(expectedTimestampInSystemTimeZone2, convertedTimestamp2);
    }

    @Test
    public void getLongFromTimestampWithTimeZoneWithoutConvertToUtc() {
        // epoch time with micro seconds is 250671662354795L (UTC)
        String timestamp = "1977-12-11 10:01:02.354795+03";
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        boolean isAdjustedToUTC = false;
        boolean isTimestampWithTimeZone = true;
        long epoch = ParquetTimestampUtilities.getLongFromTimestamp(timestamp, isAdjustedToUTC, isTimestampWithTimeZone);
        assertEquals(250671662354795L, epoch);
    }

    @Test
    public void getLongFromTimestampWithTimeZoneWithConvertToUtc() {
        // epoch time with micro seconds is 250671662354000L (UTC)
        String timestamp = "1977-12-11 10:01:02.354+03";
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        boolean isAdjustedToUTC = true;
        boolean isTimestampWithTimeZone = true;
        long epoch = ParquetTimestampUtilities.getLongFromTimestamp(timestamp, isAdjustedToUTC, isTimestampWithTimeZone);
        assertEquals(250671662354000L, epoch);
    }

    @Test
    public void getLongFromTimestampWithoutTimeZoneWithoutConvertToUtc() {
        // epoch time with micro seconds converted to UTC based on the Default timezone: 250671662030000L (1977-12-11 07:01:02.03) (Europe/Moscow)
        // epoch time with micro seconds without conversion: 250682462030000L (1977-12-11 10:01:02.03)
        String timestamp = "1977-12-11 10:01:02.03";
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow")); // +03:00
        boolean isAdjustedToUTC = false;
        boolean isTimestampWithTimeZone = false;
        long epoch = ParquetTimestampUtilities.getLongFromTimestamp(timestamp, isAdjustedToUTC, isTimestampWithTimeZone);
        assertEquals(250682462030000L, epoch);
    }

    @Test
    public void getLongFromTimestampWithoutTimeZoneWithConvertToUtc() {
        // epoch time with micro seconds converted to UTC based on the Default timezone: 250671662354500L (1977-12-11 07:01:02.3545) (Europe/Moscow)
        // epoch time with micro seconds without conversion: 25068246235400L (1977-12-11 10:01:02.3545)
        String timestamp = "1977-12-11 10:01:02.3545";
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow")); // +03:00
        boolean isAdjustedToUTC = true;
        boolean isTimestampWithTimeZone = false;
        long epoch = ParquetTimestampUtilities.getLongFromTimestamp(timestamp, isAdjustedToUTC, isTimestampWithTimeZone);
        assertEquals(250671662354500L, epoch);
    }

    @Test
    public void getTimestampFromLongWithConvertToLocalMicros() {
        // UTC: 1977-12-11 07:01:02.3545
        // Europe/Moscow time zone: 1977-12-11 10:01:02.3545
        long value = 250671662354500L;
        boolean useLocalPxfTimezoneRead = true;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.MICROS);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        assertEquals("1977-12-11 10:01:02.3545", timestamp);
    }

    @Test
    public void getTimestampFromLongWithoutConvertToLocalMicros() {
        // UTC: 1977-12-11 07:01:02.3545
        // Europe/Moscow time zone: 1977-12-11 10:01:02.3545
        long value = 250671662354500L;
        boolean useLocalPxfTimezoneRead = false;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.MICROS);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        assertEquals("1977-12-11 07:01:02.3545", timestamp);
    }

    @Test
    public void getTimestampFromLongWithConvertToLocalMillis() {
        // UTC: 1977-12-11 07:01:02.354
        // Europe/Moscow time zone: 1977-12-11 10:01:02.354
        long value = 250671662354L;
        boolean useLocalPxfTimezoneRead = true;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.MILLIS);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        assertEquals("1977-12-11 10:01:02.354", timestamp);
    }

    @Test
    public void getTimestampFromLongWithoutConvertToLocalMillis() {
        // UTC: 1977-12-11 07:01:02.354
        // Europe/Moscow time zone: 1977-12-11 10:01:02.354
        long value = 250671662354L;
        boolean useLocalPxfTimezoneRead = false;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.MILLIS);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        assertEquals("1977-12-11 07:01:02.354", timestamp);
    }

    @Test
    public void getTimestampFromLongWithConvertToLocalNanos() {
        // UTC: 1977-12-11 10:01:02.123456789
        // Europe/Moscow time zone: 1977-12-11 10:01:02.123456789
        long value = 250671662123456789L;
        boolean useLocalPxfTimezoneRead = true;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.NANOS);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        // Postgres Timestamp type support only microseconds
        assertEquals("1977-12-11 10:01:02.123456", timestamp);
    }

    @Test
    public void getTimestampFromLongWithoutConvertToLocalNanos() {
        // UTC: 1977-12-11 10:01:02.123456789
        // Europe/Moscow time zone: 1977-12-11 10:01:02.123456789
        long value = 250671662123456789L;
        boolean useLocalPxfTimezoneRead = false;
        when(group.getLong(columnIndex, repeatIndex)).thenReturn(value);
        when(type.getLogicalTypeAnnotation()).thenReturn(logicalTypeAnnotation);
        when(logicalTypeAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.NANOS);
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));
        ParquetTypeConverter converter = new Int64ParquetTypeConverter(useLocalPxfTimezoneRead);
        String timestamp = (String) converter.getValue(group, columnIndex, repeatIndex, type);
        // Postgres Timestamp type support only microsecond
        assertEquals("1977-12-11 07:01:02.123456", timestamp);
    }

    // Helper function
    private String convertUTCToCurrentSystemTimeZone(String expectedUTC) {
        // convert expectedUTC string to ZonedDateTime zdt
        LocalDateTime date = LocalDateTime.parse(expectedUTC, GreenplumDateTime.DATETIME_FORMATTER);
        ZonedDateTime zdt = ZonedDateTime.of(date, ZoneOffset.UTC);
        // convert zdt to Current Zone ID
        ZonedDateTime systemZdt = zdt.withZoneSameInstant(ZoneId.systemDefault());
        // convert date to string representation
        return systemZdt.format(GreenplumDateTime.DATETIME_FORMATTER);
    }
}