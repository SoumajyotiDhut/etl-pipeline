package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.TransformationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransformationEngine {

    private final FilterTransformation filterTransformation;
    private final MapTransformation    mapTransformation;
    private final AggregateTransformation aggregateTransformation;

    /**
     * Apply a single transformation step to a list of records.
     *
     * @param records        Input records
     * @param transformation The transformation config
     *                       (has "type" and "config" keys)
     */
    @SuppressWarnings("unchecked")
    public TransformationResult applyTransformation(
            List<DataRecord> records,
            Map<String, Object> transformation) {

        String type = (String) transformation.get("type");
        Map<String, Object> config =
                (Map<String, Object>) transformation.get("config");

        if (type == null) {
            return TransformationResult.failed(
                    "Transformation type is required");
        }
        if (config == null) {
            return TransformationResult.failed(
                    "Transformation config is required for type: " + type);
        }

        log.info("Applying transformation: {}", type);

        switch (type.toLowerCase()) {
            case "filter":
                return filterTransformation.apply(records, config);
            case "map":
                return mapTransformation.apply(records, config);
            case "aggregate":
                return aggregateTransformation.apply(records, config);
            default:
                return TransformationResult.failed(
                        "Unknown transformation type: " + type);
        }
    }

    /**
     * Apply a full chain of transformations in sequence.
     * Output of each step feeds into the next step.
     *
     * @param records         Initial records from ingestion
     * @param transformations List of transformation configs
     * @return Final records after all transformations
     */
    public TransformationChainResult applyAll(
            List<DataRecord> records,
            List<Map<String, Object>> transformations) {

        if (transformations == null || transformations.isEmpty()) {
            log.info("No transformations defined — "
                    + "returning records unchanged");
            return TransformationChainResult.builder()
                    .finalRecords(records)
                    .totalFiltered(0)
                    .totalFailed(0)
                    .stepsApplied(0)
                    .success(true)
                    .build();
        }

        List<DataRecord> current = records;
        long totalFiltered = 0;
        long totalFailed   = 0;
        int  stepsApplied  = 0;

        for (Map<String, Object> transformation : transformations) {
            TransformationResult stepResult =
                    applyTransformation(current, transformation);

            if (!stepResult.isSuccess()) {
                log.error("Transformation step failed: {}",
                        stepResult.getErrorMessage());
                return TransformationChainResult.builder()
                        .finalRecords(current)
                        .totalFiltered(totalFiltered)
                        .totalFailed(totalFailed)
                        .stepsApplied(stepsApplied)
                        .success(false)
                        .errorMessage(stepResult.getErrorMessage())
                        .build();
            }

            // Feed output into next step
            current         = stepResult.getRecords();
            totalFiltered  += stepResult.getRecordsFiltered();
            totalFailed    += stepResult.getRecordsFailed();
            stepsApplied++;

            log.info("After step {}: {} records remaining",
                    stepsApplied, current.size());
        }

        return TransformationChainResult.builder()
                .finalRecords(current)
                .totalFiltered(totalFiltered)
                .totalFailed(totalFailed)
                .stepsApplied(stepsApplied)
                .success(true)
                .build();
    }
}