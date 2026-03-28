package com.etlpipeline.ingestion;
import java.io.FileWriter;
import com.etlpipeline.model.ColumnType;
import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.IngestionResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class CsvIngestionService {

    private final SchemaInferenceService schemaInferenceService;

    /**
     * Main method — reads a CSV file and returns all records
     *
     * @param config  The source config from the pipeline definition
     * @return IngestionResult with all records and statistics
     */
    public IngestionResult ingest(Map<String, Object> config) {
        // Read config values with defaults
        String filePath   = (String) config.get("file_path");
        String delimiter  = (String) config.getOrDefault("delimiter", ",");
        boolean hasHeader = getBooleanConfig(config, "has_header", true);
        String encoding   = (String) config.getOrDefault("encoding", "utf-8");

        log.info("Starting CSV ingestion from: {}", filePath);

        // Validate file exists
        File file = new File(filePath);
        if (!file.exists()) {
            log.error("CSV file not found: {}", filePath);
            return IngestionResult.failed("File not found: " + filePath);
        }

        List<DataRecord> records      = new ArrayList<>();
        List<DataRecord> errorRecords = new ArrayList<>();
        String[] columnNames          = null;
        ColumnType[] columnTypes      = null;
        long rowNumber                = 0;

        // Determine charset
        Charset charset;
        try {
            charset = Charset.forName(encoding);
        } catch (Exception e) {
            charset = StandardCharsets.UTF_8;
            log.warn("Unknown encoding '{}', defaulting to UTF-8", encoding);
        }

        // Parse the delimiter (support tab, pipe, comma etc.)
        char delimChar = parseDelimiter(delimiter);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), charset))) {

            // ── Sample rows for schema inference ──────────────────
            List<String[]> sampleRows = new ArrayList<>();
            List<String>   allLines   = new ArrayList<>();
            String line;

            // Read all lines first (needed to sample for type inference)
            // For very large files this would be streamed — fine for Phase 1
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    allLines.add(line);
                }
            }

            if (allLines.isEmpty()) {
                return IngestionResult.failed("CSV file is empty");
            }

            // ── Process header ────────────────────────────────────
            int startRow = 0;
            if (hasHeader) {
                columnNames = parseCsvLine(allLines.get(0), delimChar);
                startRow    = 1;
            } else {
                // No header — generate column names: col_0, col_1, col_2 ...
                String[] firstRow = parseCsvLine(allLines.get(0), delimChar);
                columnNames = new String[firstRow.length];
                for (int i = 0; i < firstRow.length; i++) {
                    columnNames[i] = "col_" + i;
                }
            }

            // ── Sample up to 10 data rows for type inference ──────
            for (int i = startRow;
                 i < Math.min(startRow + 10, allLines.size()); i++) {
                sampleRows.add(parseCsvLine(allLines.get(i), delimChar));
            }
            columnTypes = schemaInferenceService
                    .inferColumnTypes(sampleRows, columnNames.length);

            log.info("CSV columns detected: {}", Arrays.toString(columnNames));
            log.info("Inferred types: {}", Arrays.toString(columnTypes));

            // ── Process each data row ─────────────────────────────
            for (int i = startRow; i < allLines.size(); i++) {
                rowNumber = i - startRow + 1;
                String dataLine = allLines.get(i);

                try {
                    String[] values = parseCsvLine(dataLine, delimChar);
                    Map<String, Object> fields = new LinkedHashMap<>();

                    for (int col = 0; col < columnNames.length; col++) {
                        String rawValue = col < values.length
                                ? values[col] : null;
                        Object converted = schemaInferenceService
                                .convertValue(rawValue, columnTypes[col]);
                        fields.put(columnNames[col], converted);
                    }

                    records.add(DataRecord.builder()
                            .fields(fields)
                            .rowNumber(rowNumber)
                            .hasError(false)
                            .build());

                } catch (Exception e) {
                    log.warn("Error parsing row {}: {}", rowNumber,
                            e.getMessage());
                    errorRecords.add(DataRecord.error(rowNumber,
                            "Parse error: " + e.getMessage()));
                }
            }

        } catch (IOException e) {
            log.error("Failed to read CSV file: {}", e.getMessage());
            return IngestionResult.failed(
                    "IO error reading file: " + e.getMessage());
        }

        log.info("CSV ingestion complete. Records: {}, Errors: {}",
                records.size(), errorRecords.size());

        return IngestionResult.builder()
                .records(records)
                .errorRecords(errorRecords)
                .totalRowsRead(rowNumber)
                .successfulRows(records.size())
                .failedRows(errorRecords.size())
                .columnNames(Arrays.asList(columnNames))
                .success(true)
                .build();
    }

    // ── Private helpers ───────────────────────────────────────────

    /**
     * Parse a single CSV line respecting quoted fields.
     * Handles: "value with, comma", 'quoted', escaped quotes
     */
    private String[] parseCsvLine(String line, char delimiter) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Handle escaped quotes ("")
                if (inQuotes && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++; // skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                fields.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString().trim());

        return fields.toArray(new String[0]);
    }

    private char parseDelimiter(String delimiter) {
        if (delimiter == null || delimiter.isEmpty()) return ',';
        switch (delimiter.toLowerCase()) {
            case "tab": case "\\t": return '\t';
            case "pipe": case "|":  return '|';
            case ";":               return ';';
            default:                return delimiter.charAt(0);
        }
    }

    private boolean getBooleanConfig(Map<String, Object> config,
                                     String key, boolean defaultValue) {
        Object val = config.get(key);
        if (val == null)             return defaultValue;
        if (val instanceof Boolean)  return (Boolean) val;
        return Boolean.parseBoolean(val.toString());
    }
    /**
     * Write failed rows to an error log file.
     * Creates a file next to the output named: filename.error.log
     */
    public void writeErrorLog(List<DataRecord> errorRecords,
                              String sourceFilePath) {
        if (errorRecords == null || errorRecords.isEmpty()) return;

        String errorLogPath = sourceFilePath + ".error.log";

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(errorLogPath))) {

            writer.write("ETL Pipeline Error Log");
            writer.newLine();
            writer.write("Source file: " + sourceFilePath);
            writer.newLine();
            writer.write("Generated at: "
                    + java.time.LocalDateTime.now());
            writer.newLine();
            writer.write("=".repeat(60));
            writer.newLine();
            writer.newLine();

            for (DataRecord errorRecord : errorRecords) {
                writer.write("Row " + errorRecord.getRowNumber()
                        + ": " + errorRecord.getErrorMessage());
                writer.newLine();
            }

            log.info("Error log written to: {}", errorLogPath);

        } catch (IOException e) {
            log.warn("Could not write error log to {}: {}",
                    errorLogPath, e.getMessage());
        }
    }
}