/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.Schema;
import io.debezium.server.bigquery.shared.BigQueryDB;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@QuarkusTest
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
class StreamRecordConverterTest extends BaseBigqueryTest {

  final String serdeWithSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema.json"));
  final String unwrapWithSchema = Files.readString(Path.of("src/test/resources/json/unwrap-with-schema.json"));
  final String unwrapWithGeomSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema_geom.json"));

  StreamRecordConverterTest() throws IOException {
  }

  @Test
  public void testGeographyValue() throws JsonProcessingException {

    StreamRecordConverter event = new StreamRecordConverter("test",
        streamConsumer.valDeserializer.deserialize("test", unwrapWithGeomSchema.getBytes()),
        null,
        streamConsumer.mapper.readTree(unwrapWithGeomSchema).get("schema"),
        null,
        streamConsumer.debeziumConfig
    );
    Schema schema = event.tableSchema();
    LOGGER.error("{}", event.tableSchema().toString());
    LOGGER.error("{}", event.convert(schema).toString());
    JSONObject converted = event.convert(schema);
    JSONObject convertedG = (JSONObject) converted.get("g");
    Assert.assertEquals(convertedG.get("srid"), 123);
    Assert.assertEquals("d35d35d34d34d34d34d34d34d34d34d347f4ddfd34d34d34d34d34d347f4dd", convertedG.get("wkb"));
  }

}
