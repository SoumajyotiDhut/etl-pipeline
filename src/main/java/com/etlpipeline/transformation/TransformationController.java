package com.etlpipeline.controller;

import com.etlpipeline.dto.ApiResponse;
import com.etlpipeline.ingestion.CsvIngestionService;
import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.IngestionResult;
import com.etlpipeline.transformation.TransformationChainResult;
import com.etlpipeline.transformation.TransformationEngine;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/transform")
@RequiredArgsConstructor
@Tag(name = "Transformation", description = "APIs for testing transformations")
public class TransformationController {

    private final CsvIngestionService csvIngestionService;
    private final TransformationEngine transformationEngine;

    /**
     * Test a full transformation chain on a CSV file.
     *
     * Request body example:
     * {
     *   "source": { "file_path": "...", "delimiter": "," },
     *   "transformations": [
     *     { "type": "filter", "config": { "condition": "quantity > 0" } },
     *     { "type": "map",    "config": { "operations": { "total": "quantity * price" } } }
     *   ]
     * }
     */
    @PostMapping("/test")
    @Operation(summary = "Test transformation chain on a CSV file")
    @SuppressWarnings("unchecked")
    public ResponseEntity<ApiResponse<Map<String, Object>>> testTransformation(
            @RequestBody Map<String, Object> request) {

        // 1. Ingest the CSV
        Map<String, Object> sourceConfig =
                (Map<String, Object>) request.get("source");
        IngestionResult ingested =
                csvIngestionService.ingest(sourceConfig);

        if (!ingested.isSuccess()) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error(
                            "Ingestion failed: " + ingested.getGeneralError()));
        }

        // 2. Apply transformations
        List<Map<String, Object>> transformations =
                (List<Map<String, Object>>) request.get("transformations");

        TransformationChainResult chainResult =
                transformationEngine.applyAll(
                        ingested.getRecords(), transformations);

        // 3. Return summary + first 10 records
        List<DataRecord> preview = chainResult.getFinalRecords()
                .subList(0, Math.min(10, chainResult.getFinalRecords().size()));

        Map<String, Object> response = Map.of(
                "inputRecords",    ingested.getSuccessfulRows(),
                "outputRecords",   chainResult.getFinalRecords().size(),
                "totalFiltered",   chainResult.getTotalFiltered(),
                "totalFailed",     chainResult.getTotalFailed(),
                "stepsApplied",    chainResult.getStepsApplied(),
                "previewRecords",  preview
        );

        return ResponseEntity.ok(
                ApiResponse.success("Transformation complete", response));
    }
}