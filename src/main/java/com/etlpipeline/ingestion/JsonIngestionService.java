package com.etlpipeline.ingestion;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.IngestionResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class JsonIngestionService {

    private final ObjectMapper objectMapper;

    /**
     * Main method — reads a JSON file and returns all records.
     * Supports three formats:
     *   "array"  → [ {}, {}, {} ]
     *   "object" → { "data": [ {}, {} ] }
     *   "jsonl"  → one JSON object per line
     */
    public IngestionResult ingest(Map<String, Object> config) {
        String filePath = (String) config.get("file_path");
        String format   = (String) config.getOrDefault("format", "array");
        String jsonPath = (String) config.get("json_path");

        log.info("Starting JSON ingestion from: {} (format: {})",
                filePath, format);

        File file = new File(filePath);
        if (!file.exists()) {
            log.error("JSON file not found: {}", filePath);
            return IngestionResult.failed("File not found: " + filePath);
        }

        try {
            List<Map<String, Object>> rawRecords;

            if ("jsonl".equalsIgnoreCase(format)) {
                rawRecords = readJsonLines(file);
            } else if ("object".equalsIgnoreCase(format)) {
                rawRecords = readJsonObject(file, jsonPath);
            } else {
                // Default: array format
                rawRecords = readJsonArray(file);
            }

            if (rawRecords == null || rawRecords.isEmpty()) {
                return IngestionResult.failed(
                        "No records found in JSON file");
            }

            // Convert each raw map into a DataRecord
            List<DataRecord> records      = new ArrayList<>();
            List<DataRecord> errorRecords = new ArrayList<>();
            long rowNumber = 0;

            // Collect all column names from the first record
            Set<String> columnSet = new LinkedHashSet<>();
            if (!rawRecords.isEmpty()) {
                columnSet.addAll(rawRecords.get(0).keySet());
            }

            for (Map<String, Object> rawRecord : rawRecords) {
                rowNumber++;
                try {
                    records.add(DataRecord.builder()
                            .fields(new LinkedHashMap<>(rawRecord))
                            .rowNumber(rowNumber)
                            .hasError(false)
                            .build());
                } catch (Exception e) {
                    log.warn("Error processing JSON record {}: {}",
                            rowNumber, e.getMessage());
                    errorRecords.add(DataRecord.error(rowNumber,
                            e.getMessage()));
                }
            }

            log.info("JSON ingestion complete. Records: {}, Errors: {}",
                    records.size(), errorRecords.size());

            return IngestionResult.builder()
                    .records(records)
                    .errorRecords(errorRecords)
                    .totalRowsRead(rowNumber)
                    .successfulRows(records.size())
                    .failedRows(errorRecords.size())
                    .columnNames(new ArrayList<>(columnSet))
                    .success(true)
                    .build();

        } catch (Exception e) {
            log.error("Failed to read JSON file: {}", e.getMessage());
            return IngestionResult.failed(
                    "Error reading JSON file: " + e.getMessage());
        }
    }

    // ── Private helpers ───────────────────────────────────────────

    /** Read a standard JSON array: [ {}, {}, {} ] */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readJsonArray(File file)
            throws IOException {
        return objectMapper.readValue(file,
                new TypeReference<List<Map<String, Object>>>() {});
    }

    /** Read JSONL format — one JSON object per line */
    private List<Map<String, Object>> readJsonLines(File file)
            throws IOException {
        List<Map<String, Object>> records = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(file),
                        StandardCharsets.UTF_8))) {

            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    Map<String, Object> record = objectMapper.readValue(
                            line,
                            new TypeReference<Map<String, Object>>() {});
                    records.add(record);
                } catch (Exception e) {
                    log.warn("Skipping invalid JSON on line {}: {}",
                            lineNum, e.getMessage());
                }
            }
        }
        return records;
    }

    /**
     * Read a JSON object that wraps an array.
     * Uses json_path like "$.data[]" to find the array.
     * For simplicity we support one level deep: "$.fieldName"
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readJsonObject(
            File file, String jsonPath) throws IOException {

        Map<String, Object> root = objectMapper.readValue(
                file,
                new TypeReference<Map<String, Object>>() {});

        if (jsonPath == null || jsonPath.isEmpty()) {
            // Try to find first array value in the root object
            for (Object value : root.values()) {
                if (value instanceof List) {
                    return (List<Map<String, Object>>) value;
                }
            }
            return Collections.emptyList();
        }

        // Simple json_path support: "$.data[]" or "$.data" → extract "data"
        String fieldName = jsonPath
                .replace("$.", "")
                .replace("[]", "")
                .trim();

        Object value = root.get(fieldName);
        if (value instanceof List) {
            return (List<Map<String, Object>>) value;
        }

        return Collections.emptyList();
    }
}