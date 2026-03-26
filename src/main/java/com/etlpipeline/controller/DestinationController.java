package com.etlpipeline.controller;

import com.etlpipeline.destination.DestinationRouter;
import com.etlpipeline.dto.ApiResponse;
import com.etlpipeline.ingestion.CsvIngestionService;
import com.etlpipeline.model.IngestionResult;
import com.etlpipeline.model.WriteResult;
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
@RequestMapping("/api/v1/destination")
@RequiredArgsConstructor
@Tag(name = "Destination", description = "APIs for testing data destinations")
public class DestinationController {

    private final CsvIngestionService  csvIngestionService;
    private final TransformationEngine transformationEngine;
    private final DestinationRouter    destinationRouter;

    /**
     * Full ETL test: ingest → transform → write to destination.
     *
     * Request body:
     * {
     *   "source":          { ... csv config ... },
     *   "transformations": [ ... ],
     *   "destination":     { "type": "file", "config": { ... } }
     * }
     */
    @PostMapping("/test")
    @Operation(summary = "Test full ETL: ingest → transform → write")
    @SuppressWarnings("unchecked")
    public ResponseEntity<ApiResponse<Map<String, Object>>> testDestination(
            @RequestBody Map<String, Object> request) {

        // 1. Ingest
        Map<String, Object> sourceConfig =
                (Map<String, Object>) request.get("source");
        IngestionResult ingested =
                csvIngestionService.ingest(sourceConfig);

        if (!ingested.isSuccess()) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error(
                            "Ingestion failed: " + ingested.getGeneralError()));
        }

        // 2. Transform
        List<Map<String, Object>> transformations =
                (List<Map<String, Object>>) request.get("transformations");

        TransformationChainResult chainResult =
                transformationEngine.applyAll(
                        ingested.getRecords(), transformations);

        // 3. Write to destination
        Map<String, Object> destination =
                (Map<String, Object>) request.get("destination");

        WriteResult writeResult = destinationRouter.route(
                chainResult.getFinalRecords(), destination);

        // 4. Return summary
        Map<String, Object> summary = Map.of(
                "recordsRead",      ingested.getSuccessfulRows(),
                "recordsFiltered",  chainResult.getTotalFiltered(),
                "recordsWritten",   writeResult.getRecordsWritten(),
                "recordsFailed",    writeResult.getRecordsFailed(),
                "destinationType",  writeResult.getDestinationType() != null
                        ? writeResult.getDestinationType() : "",
                "outputPath",       writeResult.getOutputPath() != null
                        ? writeResult.getOutputPath() : "",
                "success",          writeResult.isSuccess()
        );

        return ResponseEntity.ok(
                ApiResponse.success("ETL pipeline complete", summary));
    }
}