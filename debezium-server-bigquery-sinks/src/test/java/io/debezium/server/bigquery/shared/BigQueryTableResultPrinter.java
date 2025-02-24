package io.debezium.server.bigquery.shared;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BigQueryTableResultPrinter {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS 'UTC'").withZone(ZoneId.of("UTC"));

    public static void prettyPrintTable(TableResult tableResult) {
        if (tableResult == null) {
            System.out.println("TableResult is null.");
            return;
        }

        List<String> header = new ArrayList<>();
        tableResult.getSchema().getFields().forEach(field -> header.add(field.getName() + "(" + field.getType().toString().toLowerCase() + ")"));

        List<List<String>> rows = new ArrayList<>();
        for (FieldValueList row : tableResult.iterateAll()) {
            List<String> rowValues = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                FieldValue fieldValue = row.get(i);
                if (tableResult.getSchema().getFields().get(i).getType().getStandardType() == StandardSQLTypeName.TIMESTAMP && fieldValue.getValue() != null) {
                    Instant instant = Instant.ofEpochMilli(fieldValue.getTimestampValue() / 1000); // Convert micros to millis
                    rowValues.add(TIMESTAMP_FORMATTER.format(instant));
                } else {
                    rowValues.add(fieldValue.getValue() != null ? fieldValue.getValue().toString() : "NULL");
                }
            }
            rows.add(rowValues);
        }

        printFormattedTable(header, rows);
    }

    private static void printFormattedTable(List<String> header, List<List<String>> rows) {
        List<Integer> columnWidths = calculateColumnWidths(header, rows);

        printHorizontalLine(columnWidths);
        printHeaderRow(header, columnWidths);
        printHorizontalLine(columnWidths);
        printDataRows(rows, columnWidths);
        printHorizontalLine(columnWidths);
    }

    private static List<Integer> calculateColumnWidths(List<String> header, List<List<String>> rows) {
        List<Integer> columnWidths = new ArrayList<>();
        for (String col : header) {
            columnWidths.add(col.length());
        }

        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                columnWidths.set(i, Math.max(columnWidths.get(i), row.get(i).length()));
            }
        }
        return columnWidths;
    }

    private static void printHorizontalLine(List<Integer> columnWidths) {
        System.out.print("+");
        for (int width : columnWidths) {
            System.out.print("-" + "-".repeat(width) + "-+");
        }
        System.out.println();
    }

    private static void printHeaderRow(List<String> header, List<Integer> columnWidths) {
        System.out.print("|");
        for (int i = 0; i < header.size(); i++) {
            System.out.printf(" %-" + columnWidths.get(i) + "s |", header.get(i));
        }
        System.out.println();
    }

    private static void printDataRows(List<List<String>> rows, List<Integer> columnWidths) {
        for (List<String> row : rows) {
            System.out.print("|");
            for (int i = 0; i < row.size(); i++) {
                System.out.printf(" %-" + columnWidths.get(i) + "s |", row.get(i));
            }
            System.out.println();
        }
    }
}