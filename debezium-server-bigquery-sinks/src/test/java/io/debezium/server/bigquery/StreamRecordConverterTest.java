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
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@QuarkusTest
@Tag("integration")
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
class StreamRecordConverterTest extends BaseBigqueryTest {

  final String serdeWithSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema.json"));
  final String unwrapWithSchema = Files.readString(Path.of("src/test/resources/json/unwrap-with-schema.json"));
  final String unwrapWithGeomSchema = Files.readString(Path.of("src/test/resources/json/serde-with-schema_geom.json"));
  final String variousArrayDataTypes = Files
      .readString(Path.of("src/test/resources/json/various-array-data-types.json"));

  StreamRecordConverterTest() throws IOException {
  }

  @Test
  public void testGeographyValue() throws JsonProcessingException {

    StreamRecordConverter event = new StreamRecordConverter("test",
        streamConsumer.valDeserializer.deserialize("test", unwrapWithGeomSchema.getBytes()),
        null,
        streamConsumer.mapper.readTree(unwrapWithGeomSchema).get("schema"),
        null,
        streamConsumer.debeziumConfig);
    Schema schema = event.tableSchema();
    LOGGER.error("{}", event.tableSchema().toString());
    LOGGER.error("{}", event.convert(schema).toString());
    JSONObject converted = event.convert(schema);
    JSONObject convertedG = (JSONObject) converted.get("g");
    Assertions.assertEquals(123, convertedG.get("srid"));
    Assertions.assertEquals("d35d35d34d34d34d34d34d34d34d34d347f4ddfd34d34d34d34d34d347f4dd", convertedG.get("wkb"));
  }

  @Test
  public void testArrayValues() throws JsonProcessingException {

    StreamRecordConverter event = new StreamRecordConverter("test",
        streamConsumer.valDeserializer.deserialize("test", variousArrayDataTypes.getBytes()),
        null,
        streamConsumer.mapper.readTree(variousArrayDataTypes).get("schema"),
        null,
        streamConsumer.debeziumConfig);
    Schema schema = event.tableSchema();
    LOGGER.error("{}", event.tableSchema().toString());
    LOGGER.error("{}", event.convert(schema).toString());
    JSONObject converted = event.convert(schema);
    JSONObject after = converted.optJSONObject("after");
    Assertions.assertNotNull(after);
    JSONArray cTextArray = after.optJSONArray("c_text");
    Assertions.assertNotNull(cTextArray);
    Assertions.assertEquals(2, cTextArray.length());
    Assertions.assertEquals("Hello", cTextArray.getString(0));
    Assertions.assertEquals("World", cTextArray.getString(1));
  }

}
