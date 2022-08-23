/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.time;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.Beta;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java temporal object representations into the ISO Timestamp format.
 * its best effort to convert for all jdbc database type, some scenarios might not be handled yet.
 * it uses upstream code as much as possible.
 * {@link SchemaBuilder#string() STRING}
 *
 * @see Timestamp
 */
public class ISODateTime {
    /**
     * Used to parse values of TIMESTAMP columns. Format: 000-00-00 00:00:00.000 or 000-00-00T00:00:00.000.
     */
    public static final Pattern TIMESTAMP_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*)[\\s,T].*");

    public static final String SCHEMA_NAME = "io.debezium.time.ISODateTime";

    private ISODateTime() {
    }

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
            .name(SCHEMA_NAME)
            .version(1);
    }

    public static Schema schema() {
        return builder().build();
    }

    @Beta
    public static String toISODateTime(Object value) {
        // NOTE! assuming value is already ISO formatted Timestamp string
        if (value instanceof String) {
            final Matcher matcher = TIMESTAMP_FIELD_PATTERN.matcher((String) value);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unexpected format for TIMESTAMP column: " + value);
            }

            return (String) value;
        }
        return Conversions.toLocalDateTime(value).toString();
    }
}
