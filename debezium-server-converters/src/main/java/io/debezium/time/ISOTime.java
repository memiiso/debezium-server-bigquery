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
 * A utility for converting various Java temporal object representations into the ISO Time format.
 * its best effort to convert for all jdbc database type, some scenarios might not be handled yet.
 * it uses upstream code as much as possible.
 * {@link SchemaBuilder#string() STRING}
 *
 * @see Time
 */
public class ISOTime {
    /**
     * Used to parse values of TIME columns. Format: 000:00:00.000000.
     */
    public static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*):([0-9]*)(\\.([0-9]*))?");

    public static final String SCHEMA_NAME = "io.debezium.time.ISOTime";

    private ISOTime() {
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
    public static String toISOTime(Object value) {
        // NOTE! assuming value is already ISO formatted Time string
        if (value instanceof String) {
            final Matcher matcher = TIME_FIELD_PATTERN.matcher((String) value);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unexpected format for TIME column: " + value);
            }

            return (String) value;
        }
        return Conversions.toLocalTime(value).toString();
    }
}
