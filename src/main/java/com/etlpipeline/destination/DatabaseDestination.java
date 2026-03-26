package com.etlpipeline.destination;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.WriteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseDestination {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Write records to a database table.
     *
     * Config example:
     * {
     *   "table": "sales_summary",
     *   "write_mode": "append",
     *   "batch_size": 1000,
     *   "upsert_keys": ["product_id"]
     * }
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public WriteResult write(List<DataRecord> records,
                             Map<String, Object> config) {
        String tableName  = (String) config.get("table");
        String writeMode  = (String) config
                .getOrDefault("write_mode", "append");
        int batchSize     = getIntConfig(config, "batch_size", 1000);
        List<String> upsertKeys = (List<String>) config.get("upsert_keys");

        if (tableName == null || tableName.trim().isEmpty()) {
            return WriteResult.failed("Destination table name is required");
        }
        if (records == null || records.isEmpty()) {
            log.warn("No records to write to database");
            return WriteResult.builder()
                    .success(true)
                    .recordsWritten(0)
                    .recordsFailed(0)
                    .destinationType("database")
                    .build();
        }

        log.info("Writing {} records to table '{}' (mode: {})",
                records.size(), tableName, writeMode);

        try {
            // Get column names from first valid record
            Map<String, Object> firstRecord = records.stream()
                    .filter(r -> !r.isHasError() && r.getFields() != null)
                    .findFirst()
                    .map(DataRecord::getFields)
                    .orElse(null);

            if (firstRecord == null) {
                return WriteResult.failed("No valid records to write");
            }

            List<String> columns = new ArrayList<>(firstRecord.keySet());

            // Handle write mode
            switch (writeMode.toLowerCase()) {
                case "overwrite":
                    handleOverwrite(tableName, columns, records);
                    break;
                case "upsert":
                    return handleUpsert(tableName, columns,
                            records, upsertKeys, batchSize);
                case "append":
                default:
                    return handleAppend(tableName, columns,
                            records, batchSize);
            }

            return handleAppend(tableName, columns, records, batchSize);

        } catch (Exception e) {
            log.error("Database write failed: {}", e.getMessage());
            return WriteResult.failed(
                    "Database write error: " + e.getMessage());
        }
    }

    // ── APPEND MODE ───────────────────────────────────────────────
    private WriteResult handleAppend(String tableName,
                                     List<String> columns,
                                     List<DataRecord> records,
                                     int batchSize) {
        // Create table if it doesn't exist
        createTableIfNotExists(tableName, columns, records);

        long written = 0;
        long failed  = 0;

        // Build INSERT SQL
        String sql = buildInsertSql(tableName, columns);
        log.info("INSERT SQL: {}", sql);

        // Process in batches
        List<DataRecord> batch = new ArrayList<>();
        for (DataRecord record : records) {
            if (record.isHasError()) { failed++; continue; }
            batch.add(record);

            if (batch.size() >= batchSize) {
                written += executeBatch(sql, columns, batch);
                batch.clear();
            }
        }
        // Write remaining records
        if (!batch.isEmpty()) {
            written += executeBatch(sql, columns, batch);
        }

        log.info("Append complete. Written: {}, Failed: {}",
                written, failed);

        return WriteResult.builder()
                .success(true)
                .recordsWritten(written)
                .recordsFailed(failed)
                .destinationType("database")
                .build();
    }

    // ── OVERWRITE MODE ────────────────────────────────────────────
    private void handleOverwrite(String tableName,
                                 List<String> columns,
                                 List<DataRecord> records) {
        log.info("Overwrite mode — truncating table: {}", tableName);

        // Create table if it doesn't exist
        createTableIfNotExists(tableName, columns, records);

        // Truncate the table
        jdbcTemplate.execute("TRUNCATE TABLE " + tableName);
        log.info("Table truncated: {}", tableName);
    }

    // ── UPSERT MODE ───────────────────────────────────────────────
    private WriteResult handleUpsert(String tableName,
                                     List<String> columns,
                                     List<DataRecord> records,
                                     List<String> upsertKeys,
                                     int batchSize) {
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            log.warn("No upsert_keys defined, falling back to append");
            return handleAppend(tableName, columns, records, batchSize);
        }

        createTableIfNotExists(tableName, columns, records);

        String sql = buildUpsertSql(tableName, columns, upsertKeys);
        log.info("UPSERT SQL: {}", sql);

        long written = 0;
        long failed  = 0;

        List<DataRecord> batch = new ArrayList<>();
        for (DataRecord record : records) {
            if (record.isHasError()) { failed++; continue; }
            batch.add(record);

            if (batch.size() >= batchSize) {
                written += executeBatch(sql, columns, batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            written += executeBatch(sql, columns, batch);
        }

        return WriteResult.builder()
                .success(true)
                .recordsWritten(written)
                .recordsFailed(failed)
                .destinationType("database")
                .build();
    }

    // ── SQL BUILDERS ──────────────────────────────────────────────

    private String buildInsertSql(String tableName,
                                  List<String> columns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ",
                Collections.nCopies(columns.size(), "?"));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName, cols, placeholders);
    }

    private String buildUpsertSql(String tableName,
                                  List<String> columns,
                                  List<String> upsertKeys) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ",
                Collections.nCopies(columns.size(), "?"));

        // Columns to update on conflict (exclude upsert keys)
        List<String> updateCols = new ArrayList<>(columns);
        updateCols.removeAll(upsertKeys);

        String updateSet = String.join(", ", updateCols.stream()
                .map(c -> c + " = EXCLUDED." + c)
                .toArray(String[]::new));

        String conflictKeys = String.join(", ", upsertKeys);

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) "
                        + "ON CONFLICT (%s) DO UPDATE SET %s",
                tableName, cols, placeholders,
                conflictKeys, updateSet);
    }

    private long executeBatch(String sql, List<String> columns,
                              List<DataRecord> batch) {
        long written = 0;
        for (DataRecord record : batch) {
            try {
                Object[] params = columns.stream()
                        .map(col -> record.getFields().get(col))
                        .toArray();
                jdbcTemplate.update(sql, params);
                written++;
            } catch (Exception e) {
                log.warn("Failed to insert row {}: {}",
                        record.getRowNumber(), e.getMessage());
            }
        }
        return written;
    }

    // ── TABLE CREATION ────────────────────────────────────────────

    private void createTableIfNotExists(String tableName,
                                        List<String> columns,
                                        List<DataRecord> records) {
        // Find first valid record to infer types
        DataRecord sample = records.stream()
                .filter(r -> !r.isHasError())
                .findFirst()
                .orElse(null);

        if (sample == null) return;

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append(" (");

        for (int i = 0; i < columns.size(); i++) {
            String col   = columns.get(i);
            Object value = sample.getFields().get(col);
            String pgType = inferPostgresType(value);

            ddl.append(col).append(" ").append(pgType);
            if (i < columns.size() - 1) ddl.append(", ");
        }
        ddl.append(")");

        log.info("Creating table if not exists: {}", tableName);
        jdbcTemplate.execute(ddl.toString());
    }

    private String inferPostgresType(Object value) {
        if (value instanceof Long
                || value instanceof Integer)    return "BIGINT";
        if (value instanceof BigDecimal
                || value instanceof Double)     return "NUMERIC(20,4)";
        if (value instanceof Boolean)    return "BOOLEAN";
        if (value instanceof LocalDateTime) return "TIMESTAMP";
        return "TEXT";
    }

    private int getIntConfig(Map<String, Object> config,
                             String key, int defaultValue) {
        Object val = config.get(key);
        if (val == null)            return defaultValue;
        if (val instanceof Integer) return (Integer) val;
        try { return Integer.parseInt(val.toString()); }
        catch (Exception e)         { return defaultValue; }
    }
}