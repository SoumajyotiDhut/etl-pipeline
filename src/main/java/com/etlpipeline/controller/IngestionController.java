package com.etlpipeline.controller;

import com.etlpipeline.dto.ApiResponse;
import com.etlpipeline.ingestion.CsvIngestionService;
import com.etlpipeline.ingestion.JsonIngestionService;
import com.etlpipeline.model.IngestionResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingest")
@RequiredArgsConstructor
@Tag(name = "Ingestion", description = "APIs for testing data ingestion")
public class IngestionController {

    private final CsvIngestionService csvIngestionService;
    private final JsonIngestionService jsonIngestionService;

    // POST /api/v1/ingest/csv
    @PostMapping("/csv")
    @Operation(summary = "Test CSV ingestion with a source config")
    public ResponseEntity<ApiResponse<IngestionResult>> ingestCsv(
            @RequestBody Map<String, Object> sourceConfig) {

        IngestionResult result = csvIngestionService.ingest(sourceConfig);
        return ResponseEntity.ok(
                ApiResponse.success("CSV ingestion complete", result));
    }

    // POST /api/v1/ingest/json
    @PostMapping("/json")
    @Operation(summary = "Test JSON ingestion with a source config")
    public ResponseEntity<ApiResponse<IngestionResult>> ingestJson(
            @RequestBody Map<String, Object> sourceConfig) {

        IngestionResult result = jsonIngestionService.ingest(sourceConfig);
        return ResponseEntity.ok(
                ApiResponse.success("JSON ingestion complete", result));
    }
}