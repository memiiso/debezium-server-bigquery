/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.ISODate;
import io.debezium.time.ISODateTime;
import io.debezium.time.ISOTime;
import io.debezium.util.Collect;

import java.sql.Types;
import java.util.List;
import java.util.Properties;

import com.google.common.annotations.Beta;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Converter to convert temporal types {DATE, TIME, TIMESTAMP} to ISO string
 * its best effort to convert for all jdbc database type, some scenarios might not be handled yet.
 * it uses upstream code as much as possible.
 */
@Beta
public class TemporalToISOStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final String FALLBACK_NULL = null;
    private static final List<Integer> DATETIME_FAMILY = Collect.arrayListOf(
        Types.TIMESTAMP,
        1114 // POSTGRESQL PgOid.TIMESTAMP // TIMESTAMP WITHOUT TIME ZONE
    );
    private static final List<Integer> DATE_FAMILY = Collect.arrayListOf(Types.DATE);
    private static final List<Integer> TIME_FAMILY = Collect.arrayListOf(Types.TIME);
    private static final List<String> IGNORE_TZ_TYPES = Collect.arrayListOf("TIMETZ", "TIMESTAMPTZ");
    private static final Logger LOGGER = LoggerFactory.getLogger(TemporalToISOStringConverter.class);

    private static SchemaBuilder getFieldSchema(RelationalColumn field) {
        if (DATETIME_FAMILY.contains(field.jdbcType())) {
            return ISODateTime.builder();
        }
        if (DATE_FAMILY.contains(field.jdbcType())) {
            return ISODate.builder();
        }
        if (TIME_FAMILY.contains(field.jdbcType())) {
            return ISOTime.builder();
        }
        throw new RuntimeException("Unexpected field type " + field.jdbcType() + " " + field.typeName());
    }

    @Override
    public void configure(Properties props) {
        //
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!((DATETIME_FAMILY.contains(field.jdbcType()) ||
            DATE_FAMILY.contains(field.jdbcType()) ||
            TIME_FAMILY.contains(field.jdbcType())) &&
            !IGNORE_TZ_TYPES.contains(field.typeName().toUpperCase()))
        ) {
            return;
        }
        registration.register(getFieldSchema(field), x -> {
            if (x == null) {
                if (field.isOptional()) {
                    return null;
                } else if (field.hasDefaultValue()) {
                    return field.defaultValue();
                }
                return FALLBACK_NULL;
            }
            if (DATETIME_FAMILY.contains(field.jdbcType())) {
                return ISODateTime.toISODateTime(x);
            }
            if (DATE_FAMILY.contains(field.jdbcType())) {
                return ISODate.toISODate(x);
            }
            if (TIME_FAMILY.contains(field.jdbcType())) {
                return ISOTime.toISOTime(x);
            }
            throw new IllegalArgumentException("Unable to convert to LocalDate from unexpected value '" + x + "' of type " + x.getClass().getName());
        });
    }

}
