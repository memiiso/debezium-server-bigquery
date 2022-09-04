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
 * A utility for converting various Java temporal object representations into the ISO date format.
 * its best effort to convert for all jdbc database type, some scenarios might not be handled yet.
 * it uses upstream code as much as possible.
 * {@link SchemaBuilder#string() STRING}
 *
 * @see Date
 */
public class ISODate {


    /**
     * Used to parse values of DATE columns. Format: 000-00-00.
     */
    public static final Pattern DATE_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*)");


    public static final String SCHEMA_NAME = "io.debezium.time.ISODate";

    private ISODate() {
    }

    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
            .name(SCHEMA_NAME)
            .version(1);
    }

    public static Schema schema() {
        return builder().build();
    }


    /**
     * @param value
     * @return
     */
    @Beta
    public static String toISODate(Object value) {
        // NOTE! assuming value is already ISO formatted Date string
        if (value instanceof String) {
            final Matcher matcher = DATE_FIELD_PATTERN.matcher((String) value);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unexpected format for DATE column: " + value);
            }

            return (String) value;
        }
        return Conversions.toLocalDate(value).toString();
    }
}
