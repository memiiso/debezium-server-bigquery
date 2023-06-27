/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import io.debezium.server.bigquery.DebeziumBigqueryEvent;

import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * helper class used to generate test change events
 *
 * @author Ismail Simsek
 */
public class DebeziumBigqueryEventBuilder {

  ObjectNode payload = JsonNodeFactory.instance.objectNode();
  ObjectNode keyPayload = JsonNodeFactory.instance.objectNode();
  String destination = "test";

  public DebeziumBigqueryEventBuilder() {
  }

  public DebeziumBigqueryEventBuilder destination(String destination) {
    this.destination = destination;
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String parentFieldName, String name, String val) {
    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String parentFieldName, String name, int val) {
    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String parentFieldName, String name, boolean val) {

    ObjectNode nestedField = JsonNodeFactory.instance.objectNode();
    if (this.payload.has(parentFieldName)) {
      nestedField = (ObjectNode) this.payload.get(parentFieldName);
    }
    nestedField.put(name, val);
    this.payload.set(parentFieldName, nestedField);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String name, int val) {
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String name, String val) {
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String name, long val) {
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String name, double val) {
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addField(String name, boolean val) {
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addKeyField(String name, int val) {
    keyPayload.put(name, val);
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEventBuilder addKeyField(String name, String val) {
    keyPayload.put(name, val);
    payload.put(name, val);
    return this;
  }

  public DebeziumBigqueryEvent build() {
    return new DebeziumBigqueryEvent(
        this.destination,
        payload,
        keyPayload,
        this.valueSchema(),
        this.keySchema()
    );
  }

  private ObjectNode valueSchema() {
    return getSchema(payload);
  }

  private ObjectNode keySchema() {
    return getSchema(keyPayload);
  }

  private ObjectNode getSchema(ObjectNode node) {
    ObjectNode schema = JsonNodeFactory.instance.objectNode();

    ArrayNode fs = getSchemaFields(node);
    if (fs.isEmpty()) {
      return null;
    } else {
      schema.put("type", "struct");
      schema.set("fields", fs);
      return schema;
    }
  }

  private ArrayNode getSchemaFields(ObjectNode node) {
    ArrayNode fields = JsonNodeFactory.instance.arrayNode();
    Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> field = iter.next();

      ObjectNode schemaField = JsonNodeFactory.instance.objectNode();
      if (field.getValue().isContainerNode()) {
        schemaField.put("type", "struct");
        schemaField.set("fields", getSchemaFields((ObjectNode) field.getValue()));
      } else if (field.getValue().isInt()) {
        schemaField.put("type", "int32");
      } else if (field.getValue().isLong()) {
        schemaField.put("type", "int64");
      } else if (field.getValue().isBoolean()) {
        schemaField.put("type", "boolean");
      } else if (field.getValue().isTextual()) {
        schemaField.put("type", "string");
      } else if (field.getValue().isFloat()) {
        schemaField.put("type", "float64");
      }
      if (keyPayload.has(field.getKey())) {
        schemaField.put("optional", false);
      } else {
        schemaField.put("optional", true);
      }
      schemaField.put("field", field.getKey());
      fields.add(schemaField);
    }

    return fields;
  }


}