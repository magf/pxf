package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.greenplum.pxf.api.GreenplumDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.*;
import java.util.Base64;

import org.apache.parquet.io.api.Binary;

import static org.greenplum.pxf.plugins.hdfs.parquet.ParquetConstant.*;

public class ParquetTimestampUtilities {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetTimestampUtilities.class);

    /**
     * Parses the dateString and returns the number of days between the unix
     * epoch (1 January 1970) until the given date in the dateString
     */
    public static int getDaysFromEpochFromDateString(String dateString) {
        LocalDate date = LocalDate.parse(dateString, GreenplumDateTime.DATE_FORMATTER);
        return (int) date.toEpochDay();
    }

    public static long getLongFromTimestamp(String timestampString, boolean isAdjustedToUTC, boolean isTimestampWithTimeZone) {
        if (isTimestampWithTimeZone) {
            // We receive a timestamp string with time zone offset from GPDB
            OffsetDateTime date = OffsetDateTime.parse(timestampString, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
            ZonedDateTime zdt = date.toZonedDateTime();
            return getEpochWithMicroSeconds(zdt, date.getNano());
        } else {
            // We receive a timestamp string from GPDB in the server timezone
            // If isAdjustedToUTC = true we convert it to the UTC using local pxf server timezone and save it in the parquet as UTC
            // If isAdjustedToUTC = false we don't convert timestamp to the instant and save it as is
            ZoneId zoneId = isAdjustedToUTC ? ZoneId.systemDefault() : ZoneOffset.UTC;
            LocalDateTime date = LocalDateTime.parse(timestampString, GreenplumDateTime.DATETIME_FORMATTER);
            ZonedDateTime zdt = ZonedDateTime.of(date, zoneId);
            return getEpochWithMicroSeconds(zdt, date.getNano());
        }
    }

    public static String getTimestampFromLong(long value, LogicalTypeAnnotation.TimeUnit timeUnit, boolean useLocalTimezone) {
        long seconds = 0L;
        long nanoseconds = 0L;

        // Parquet timestamp doesn't contain time zone
        // If useLocalTimezone = true we convert timestamp to an instant of the current PXF server timezone
        // If useLocalTimezone = false we send timestamp to GP as is
        ZoneId zoneId = useLocalTimezone ? ZoneId.systemDefault() : ZoneOffset.UTC;

        switch (timeUnit) {
            case MILLIS:
                seconds = value / SECOND_IN_MILLIS;
                nanoseconds = (value % SECOND_IN_MILLIS) * NANOS_IN_MILLIS;
                break;
            case MICROS:
                seconds = value / SECOND_IN_MICROS;
                nanoseconds = (value % SECOND_IN_MICROS) * NANOS_IN_MICROS;
                break;
            case NANOS:
                // Greenplum timestamp type has a minimum resolution 1 microsecond
                seconds = value / SECOND_IN_NANOS;
                nanoseconds = (value % SECOND_IN_NANOS);
                break;
            default:
                break;
        }
        Instant instant = Instant.ofEpochSecond(seconds, nanoseconds);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zoneId);
        return localDateTime.format(GreenplumDateTime.DATETIME_FORMATTER);
    }

    /**
     * Converts a timestamp string to a INT96 byte array.
     * Supports microseconds for timestamps
     */
    public static Binary getBinaryFromTimestamp(String timestampString) {
        // We receive a timestamp string from GPDB in the server timezone
        // We convert it to an instant of the current server timezone
        LocalDateTime date = LocalDateTime.parse(timestampString, GreenplumDateTime.DATETIME_FORMATTER);
        ZonedDateTime zdt = ZonedDateTime.of(date, ZoneId.systemDefault());
        return getBinaryFromZonedDateTime(timestampString, zdt);
    }

    /**
     * Converts a "timestamp with time zone" string to a INT96 byte array.
     * Supports microseconds for timestamps
     *
     * @param timestampWithTimeZoneString the greenplum string of the timestamp with the time zone
     * @return Binary format of the timestamp with time zone string
     */
    public static Binary getBinaryFromTimestampWithTimeZone(String timestampWithTimeZoneString) {
        OffsetDateTime date = OffsetDateTime.parse(timestampWithTimeZoneString, GreenplumDateTime.DATETIME_WITH_TIMEZONE_FORMATTER);
        ZonedDateTime zdt = date.toZonedDateTime();
        return getBinaryFromZonedDateTime(timestampWithTimeZoneString, zdt);
    }

    // Convert parquet byte array to java timestamp IN LOCAL SERVER'S TIME ZONE
    public static String bytesToTimestamp(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = byteBuffer.getLong();
        long julianDay = byteBuffer.getInt();
        long unixTimeMs = (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;

        Instant instant = Instant.ofEpochMilli(unixTimeMs); // time read from Parquet is in UTC
        instant = instant.plusNanos(timeOfDayNanos);
        String timestamp = instant.atZone(ZoneId.systemDefault()).format(GreenplumDateTime.DATETIME_FORMATTER);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Converted bytes: {} to date: {} from: julianDays {}, timeOfDayNanos {}, unixTimeMs {}",
                    Base64.getEncoder().encodeToString(bytes),
                    timestamp, julianDay, timeOfDayNanos, unixTimeMs);
        }
        return timestamp;
    }

    // Helper method that takes a ZonedDateTime object and return it as nano time as long type
    private static long getEpochWithMicroSeconds(ZonedDateTime zdt, int nanoOfSecond) {
        long microSeconds = zdt.toEpochSecond() * SECOND_IN_MICROS;
        return microSeconds + nanoOfSecond / NANOS_IN_MICROS;
    }

    // Helper method that takes a ZonedDateTime object and return it as nano time in binary form (UTC)
    private static Binary getBinaryFromZonedDateTime(String timestampString, ZonedDateTime zdt) {
        long timeMicros = (zdt.toEpochSecond() * SECOND_IN_MICROS) + zdt.getNano() / NANOS_IN_MICROS;
        long daysSinceEpoch = timeMicros / MICROS_IN_DAY;
        int julianDays = (int) (JULIAN_EPOCH_OFFSET_DAYS + daysSinceEpoch);
        long timeOfDayNanos = (timeMicros % MICROS_IN_DAY) * NANOS_IN_MICROS;
        LOG.debug("Converted timestamp: {} to julianDays: {}, timeOfDayNanos: {}", timestampString, julianDays, timeOfDayNanos);
        return new NanoTime(julianDays, timeOfDayNanos).toBinary();
    }
}
