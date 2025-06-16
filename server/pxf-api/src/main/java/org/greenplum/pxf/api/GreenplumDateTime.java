package org.greenplum.pxf.api;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Provides formatters for Greengage DateTime
 */
public class GreenplumDateTime {
    public static final int NANOS_IN_MICROS = 1000;
    public static final int NANOS_IN_MILLIS = NANOS_IN_MICROS * 1000;

    public static final String DATE_FORMATTER_BASE_PATTERN = "yyyy-MM-dd";

    public static final String DATETIME_FORMATTER_BASE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * The date formatter for Greengage values
     */
    public static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern(DATE_FORMATTER_BASE_PATTERN)
                    .toFormatter();

    /**
     * Supports date times with the format yyyy-MM-dd HH:mm:ss and
     * optional microsecond
     */
    public static final DateTimeFormatter DATETIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern(DATETIME_FORMATTER_BASE_PATTERN)
                    // Parsing nanos in strict mode, the number of parsed digits must be between 0 and 9 (nanosecond support)
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /**
     * Supports times with the format HH:mm:ss and
     * optional microsecond
     */
    public static final DateTimeFormatter TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    // Parsing nanos in strict mode, the number of parsed digits must be between 0 and 9 (nanosecond support)
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /**
     * Supports date times with timezone and optional microsecond
     */
    private static final DateTimeFormatter optionalFormatter = new DateTimeFormatterBuilder().appendOffset("+HH:mm", "Z").toFormatter();
    public static final DateTimeFormatter DATETIME_WITH_TIMEZONE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DATETIME_FORMATTER)
                    // Make the mm optional since Greengage will only send HH if mm == 00
                    .appendOptional(optionalFormatter)
                    .toFormatter();
}
