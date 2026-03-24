package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.TransformationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class FilterTransformation {

    private final ExpressionEvaluator evaluator;

    /**
     * Filter records based on a condition.
     * Records that do NOT match the condition are removed.
     *
     * Config example:
     * { "condition": "quantity > 0" }
     */
    public TransformationResult apply(List<DataRecord> records,
                                      Map<String, Object> config) {
        String condition = (String) config.get("condition");

        if (condition == null || condition.trim().isEmpty()) {
            log.warn("Filter transformation has no condition — "
                    + "keeping all records");
            return TransformationResult.builder()
                    .records(records)
                    .recordsFiltered(0)
                    .recordsFailed(0)
                    .transformationType("filter")
                    .success(true)
                    .build();
        }

        log.info("Applying filter: {}", condition);

        List<DataRecord> kept    = new ArrayList<>();
        List<DataRecord> removed = new ArrayList<>();

        for (DataRecord record : records) {
            // Skip records that already have errors
            if (record.isHasError()) {
                removed.add(record);
                continue;
            }

            try {
                boolean matches = evaluator.evaluateCondition(
                        condition, record.getFields());
                if (matches) {
                    kept.add(record);
                } else {
                    removed.add(record);
                }
            } catch (Exception e) {
                log.warn("Error evaluating filter on row {}: {}",
                        record.getRowNumber(), e.getMessage());
                removed.add(record);
            }
        }

        log.info("Filter complete. Kept: {}, Removed: {}",
                kept.size(), removed.size());

        return TransformationResult.builder()
                .records(kept)
                .recordsFiltered(removed.size())
                .recordsFailed(0)
                .transformationType("filter")
                .success(true)
                .build();
    }
}