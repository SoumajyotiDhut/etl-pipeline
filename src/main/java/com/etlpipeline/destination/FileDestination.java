package com.etlpipeline.destination;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.WriteResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

@Service
@Slf4j
public class FileDestination {

    /**
     * Write records to a CSV or JSON file.
     *
     * Config example:
     * {
     *   "output_path": "/outputs/sales_summary.csv",
     *   "format": "csv",
     *   "delimiter": ",",
     *   "include_header": true,
     *   "compression": "gzip"
     * }
     */
    public WriteResult write(List<DataRecord> records,
                             Map<String, Object> config) {
        String outputPath    = (String) config.get("output_path");
        String format        = (String) config
                .getOrDefault("format", "csv");
        String compression   = (String) config.get("compression");

        if (outputPath == null || outputPath.trim().isEmpty()) {
            return WriteResult.failed("output_path is required");
        }
        if (records == null || records.isEmpty()) {
            log.warn("No records to write to file");
            return WriteResult.builder()
                    .success(true)
                    .recordsWritten(0)
                    .recordsFailed(0)
                    .destinationType("file")
                    .outputPath(outputPath)
                    .build();
        }

        // Add .gz extension if compression is enabled
        if ("gzip".equalsIgnoreCase(compression)
                && !outputPath.endsWith(".gz")) {
            outputPath = outputPath + ".gz";
        }

        // Create parent directories if they don't exist
        try {
            File outputFile = new File(outputPath);
            if (outputFile.getParentFile() != null) {
                outputFile.getParentFile().mkdirs();
            }
        } catch (Exception e) {
            log.warn("Could not create output directory: {}",
                    e.getMessage());
        }

        log.info("Writing {} records to file: {} (format: {})",
                records.size(), outputPath, format);

        try {
            WriteResult result;
            if ("json".equalsIgnoreCase(format)) {
                result = writeJson(records, outputPath, compression);
            } else {
                result = writeCsv(records, config,
                        outputPath, compression);
            }
            return result;

        } catch (Exception e) {
            log.error("File write failed: {}", e.getMessage());
            return WriteResult.failed(
                    "File write error: " + e.getMessage());
        }
    }

    // ── CSV WRITER ────────────────────────────────────────────────

    private WriteResult writeCsv(List<DataRecord> records,
                                 Map<String, Object> config,
                                 String outputPath,
                                 String compression)
            throws IOException {

        String delimiter     = (String) config
                .getOrDefault("delimiter", ",");
        boolean includeHeader = getBooleanConfig(
                config, "include_header", true);

        // Get column names from first valid record
        List<String> columns = getColumnNames(records);
        if (columns.isEmpty()) {
            return WriteResult.failed("No columns found in records");
        }

        long written = 0;
        long failed  = 0;

        OutputStream outputStream = createOutputStream(
                outputPath, compression);

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(outputStream,
                        StandardCharsets.UTF_8))) {

            // Write header row
            if (includeHeader) {
                writer.write(String.join(delimiter, columns));
                writer.newLine();
            }

            // Write each record
            for (DataRecord record : records) {
                if (record.isHasError()) { failed++; continue; }

                try {
                    List<String> values = new ArrayList<>();
                    for (String col : columns) {
                        Object val = record.getFields().get(col);
                        values.add(formatCsvValue(val, delimiter));
                    }
                    writer.write(String.join(delimiter, values));
                    writer.newLine();
                    written++;
                } catch (Exception e) {
                    log.warn("Failed to write row {}: {}",
                            record.getRowNumber(), e.getMessage());
                    failed++;
                }
            }
        }

        log.info("CSV write complete. Written: {}, Failed: {}",
                written, failed);

        return WriteResult.builder()
                .success(true)
                .recordsWritten(written)
                .recordsFailed(failed)
                .destinationType("file")
                .outputPath(outputPath)
                .build();
    }

    // ── JSON WRITER ───────────────────────────────────────────────

    private WriteResult writeJson(List<DataRecord> records,
                                  String outputPath,
                                  String compression)
            throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        List<Map<String, Object>> output = new ArrayList<>();
        long failed = 0;

        for (DataRecord record : records) {
            if (record.isHasError()) { failed++; continue; }
            output.add(record.getFields());
        }

        OutputStream outputStream = createOutputStream(
                outputPath, compression);

        try (outputStream) {
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(outputStream, output);
        }

        long written = output.size();
        log.info("JSON write complete. Written: {}, Failed: {}",
                written, failed);

        return WriteResult.builder()
                .success(true)
                .recordsWritten(written)
                .recordsFailed(failed)
                .destinationType("file")
                .outputPath(outputPath)
                .build();
    }

    // ── HELPERS ───────────────────────────────────────────────────

    private OutputStream createOutputStream(String outputPath,
                                            String compression)
            throws IOException {
        OutputStream base = Files.newOutputStream(
                Paths.get(outputPath));
        if ("gzip".equalsIgnoreCase(compression)) {
            return new GZIPOutputStream(base);
        }
        return base;
    }

    private List<String> getColumnNames(List<DataRecord> records) {
        return records.stream()
                .filter(r -> !r.isHasError() && r.getFields() != null)
                .findFirst()
                .map(r -> new ArrayList<>(r.getFields().keySet()))
                .orElse(new ArrayList<>());
    }

    private String formatCsvValue(Object value, String delimiter) {
        if (value == null) return "";
        String str = value.toString();
        // Wrap in quotes if value contains delimiter, newline, or quotes
        if (str.contains(delimiter)
                || str.contains("\n")
                || str.contains("\"")) {
            str = "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    private boolean getBooleanConfig(Map<String, Object> config,
                                     String key,
                                     boolean defaultValue) {
        Object val = config.get(key);
        if (val == null)            return defaultValue;
        if (val instanceof Boolean) return (Boolean) val;
        return Boolean.parseBoolean(val.toString());
    }
}
