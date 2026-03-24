package com.etlpipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataRecord {

    // Stores one row of data as key-value pairs
    // Example: {id=1, name=John, amount=500.0}
    private Map<String, Object> fields;

    // Track which row number this came from (for error reporting)
    private long rowNumber;

    // Whether this record had any errors during ingestion
    private boolean hasError;

    // Error message if something went wrong
    private String errorMessage;

    // Helper method to get a field value by name
    public Object getField(String fieldName) {
        return fields != null ? fields.get(fieldName) : null;
    }

    // Helper method to set a field value
    public void setField(String fieldName, Object value) {
        if (fields == null) {
            fields = new HashMap<>();
        }
        fields.put(fieldName, value);
    }

    // Helper to create a clean copy with new fields
    // (used during transformations)
    public DataRecord withFields(Map<String, Object> newFields) {
        return DataRecord.builder()
                .fields(new HashMap<>(newFields))
                .rowNumber(this.rowNumber)
                .hasError(false)
                .build();
    }

    // Helper to create an error record
    public static DataRecord error(long rowNumber, String errorMessage) {
        return DataRecord.builder()
                .fields(new HashMap<>())
                .rowNumber(rowNumber)
                .hasError(true)
                .errorMessage(errorMessage)
                .build();
    }
}