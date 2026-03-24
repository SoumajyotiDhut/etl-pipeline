package com.etlpipeline.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class IngestionResult {

    // All successfully read records
    private List<DataRecord> records;

    // Records that failed to parse
    private List<DataRecord> errorRecords;

    // Statistics
    private long totalRowsRead;
    private long successfulRows;
    private long failedRows;

    // Column names detected from the file
    private List<String> columnNames;

    // Any general error message (e.g. file not found)
    private String generalError;

    // Whether ingestion completed without fatal errors
    private boolean success;

    // Helper to create a failed result
    public static IngestionResult failed(String errorMessage) {
        return IngestionResult.builder()
                .success(false)
                .generalError(errorMessage)
                .totalRowsRead(0)
                .successfulRows(0)
                .failedRows(0)
                .build();
    }
}