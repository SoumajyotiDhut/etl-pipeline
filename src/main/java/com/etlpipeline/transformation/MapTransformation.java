package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.TransformationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class MapTransformation {

    private final ExpressionEvaluator evaluator;

    /**
     * Map transformation — rename columns, derive new columns,
     * or transform values.
     *
     * Config example:
     * {
     *   "operations": {
     *     "full_name":   "CONCAT(first_name, ' ', last_name)",
     *     "email":       "LOWER(email)",
     *     "total":       "quantity * price",
     *     "customer_id": "id"
     *   }
     * }
     */
    @SuppressWarnings("unchecked")
    public TransformationResult apply(List<DataRecord> records,
                                      Map<String, Object> config) {
        Map<String, String> operations =
                (Map<String, String>) config.get("operations");

        if (operations == null || operations.isEmpty()) {
            log.warn("Map transformation has no operations — "
                    + "returning records unchanged");
            return TransformationResult.builder()
                    .records(records)
                    .recordsFiltered(0)
                    .recordsFailed(0)
                    .transformationType("map")
                    .success(true)
                    .build();
        }

        log.info("Applying map transformation with {} operations",
                operations.size());

        List<DataRecord> result    = new ArrayList<>();
        long failedCount           = 0;

        for (DataRecord record : records) {
            if (record.isHasError()) {
                result.add(record);
                continue;
            }

            try {
                // Start with a copy of existing fields
                Map<String, Object> newFields =
                        new LinkedHashMap<>(record.getFields());

                // Apply each operation
                for (Map.Entry<String, String> op : operations.entrySet()) {
                    String targetColumn = op.getKey();
                    String expression   = op.getValue();

                    try {
                        Object value = evaluator.evaluateExpression(
                                expression, newFields);
                        newFields.put(targetColumn, value);

                    } catch (Exception e) {
                        log.warn("Error evaluating expression '{}' "
                                        + "on row {}: {}",
                                expression,
                                record.getRowNumber(),
                                e.getMessage());
                        newFields.put(targetColumn, null);
                    }
                }

                result.add(record.withFields(newFields));

            } catch (Exception e) {
                log.warn("Map transformation failed on row {}: {}",
                        record.getRowNumber(), e.getMessage());
                result.add(DataRecord.error(record.getRowNumber(),
                        "Map error: " + e.getMessage()));
                failedCount++;
            }
        }

        log.info("Map transformation complete. Processed: {}, Failed: {}",
                result.size(), failedCount);

        return TransformationResult.builder()
                .records(result)
                .recordsFiltered(0)
                .recordsFailed(failedCount)
                .transformationType("map")
                .success(true)
                .build();
    }
}