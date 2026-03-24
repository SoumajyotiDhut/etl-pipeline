package com.etlpipeline.ingestion;

import com.etlpipeline.model.ColumnType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class SchemaInferenceService {

    // Common datetime formats to try during parsing
    private static final List<DateTimeFormatter> DATE_FORMATTERS = Arrays.asList(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy")
    );

    // Values that should be treated as null
    private static final List<String> NULL_VALUES = Arrays.asList(
            "", "null", "NULL", "N/A", "n/a", "NA", "none", "NONE"
    );

    /**
     * Infer the type of a single value by trying to parse it
     */
    public ColumnType inferType(String value) {
        if (value == null || NULL_VALUES.contains(value.trim())) {
            return ColumnType.UNKNOWN;
        }

        String trimmed = value.trim();

        // Try boolean first
        if (trimmed.equalsIgnoreCase("true") ||
                trimmed.equalsIgnoreCase("false")) {
            return ColumnType.BOOLEAN;
        }

        // Try integer
        try {
            Long.parseLong(trimmed);
            return ColumnType.INTEGER;
        } catch (NumberFormatException ignored) {}

        // Try decimal
        try {
            new BigDecimal(trimmed);
            return ColumnType.DECIMAL;
        } catch (NumberFormatException ignored) {}

        // Try datetime
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                LocalDateTime.parse(trimmed, formatter);
                return ColumnType.DATETIME;
            } catch (DateTimeParseException ignored) {}
        }

        // Default to string
        return ColumnType.STRING;
    }

    /**
     * Convert a raw string value to the correct Java type
     * based on the inferred or provided column type
     */
    public Object convertValue(String value, ColumnType type) {
        if (value == null || NULL_VALUES.contains(value.trim())) {
            return null;
        }

        String trimmed = value.trim();

        try {
            switch (type) {
                case INTEGER:
                    return Long.parseLong(trimmed);

                case DECIMAL:
                    return new BigDecimal(trimmed);

                case BOOLEAN:
                    return Boolean.parseBoolean(trimmed);

                case DATETIME:
                    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
                        try {
                            return LocalDateTime.parse(trimmed, formatter);
                        } catch (DateTimeParseException ignored) {}
                    }
                    return trimmed; // fallback to string

                case STRING:
                default:
                    return trimmed;
            }
        } catch (Exception e) {
            log.warn("Could not convert value '{}' to type {}, "
                    + "returning as string", value, type);
            return trimmed;
        }
    }

    /**
     * Auto-infer types for all columns by sampling the first N rows
     */
    public ColumnType[] inferColumnTypes(List<String[]> sampleRows,
                                         int columnCount) {
        ColumnType[] types = new ColumnType[columnCount];

        // Initialize all as UNKNOWN
        for (int i = 0; i < columnCount; i++) {
            types[i] = ColumnType.UNKNOWN;
        }

        // Sample up to 10 rows to determine types
        int sampleSize = Math.min(sampleRows.size(), 10);

        for (int col = 0; col < columnCount; col++) {
            ColumnType dominantType = ColumnType.UNKNOWN;

            for (int row = 0; row < sampleSize; row++) {
                String[] rowData = sampleRows.get(row);
                if (col < rowData.length) {
                    ColumnType inferredType = inferType(rowData[col]);
                    if (inferredType != ColumnType.UNKNOWN) {
                        // Use the first non-unknown type we find
                        dominantType = inferredType;
                        break;
                    }
                }
            }

            types[col] = dominantType == ColumnType.UNKNOWN
                    ? ColumnType.STRING
                    : dominantType;
        }

        return types;
    }
}