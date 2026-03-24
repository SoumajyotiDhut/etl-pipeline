package com.etlpipeline.controller;

import com.etlpipeline.dto.ApiResponse;
import com.etlpipeline.dto.PipelineRequest;
import com.etlpipeline.dto.PipelineResponse;
import com.etlpipeline.service.PipelineService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/pipelines")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Pipeline Management",
        description = "APIs for creating and managing ETL pipelines")
public class PipelineController {

    private final PipelineService pipelineService;

    // POST /api/v1/pipelines
    @PostMapping
    @Operation(summary = "Create a new pipeline")
    public ResponseEntity<ApiResponse<PipelineResponse>> createPipeline(
            @Valid @RequestBody PipelineRequest request) {

        PipelineResponse response = pipelineService.createPipeline(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ApiResponse.success("Pipeline created successfully",
                        response));
    }

    // GET /api/v1/pipelines/{pipelineId}
    @GetMapping("/{pipelineId}")
    @Operation(summary = "Get a pipeline by ID")
    public ResponseEntity<ApiResponse<PipelineResponse>> getPipeline(
            @PathVariable String pipelineId) {

        PipelineResponse response = pipelineService.getPipeline(pipelineId);
        return ResponseEntity.ok(
                ApiResponse.success("Pipeline retrieved successfully", response));
    }

    // GET /api/v1/pipelines?tenantId=tenant_123
    @GetMapping
    @Operation(summary = "Get all pipelines, optionally filter by tenant")
    public ResponseEntity<ApiResponse<List<PipelineResponse>>> getAllPipelines(
            @RequestParam(required = false) String tenantId) {

        List<PipelineResponse> responses =
                pipelineService.getAllPipelines(tenantId);
        return ResponseEntity.ok(
                ApiResponse.success("Pipelines retrieved successfully", responses));
    }

    // PUT /api/v1/pipelines/{pipelineId}
    @PutMapping("/{pipelineId}")
    @Operation(summary = "Update an existing pipeline")
    public ResponseEntity<ApiResponse<PipelineResponse>> updatePipeline(
            @PathVariable String pipelineId,
            @Valid @RequestBody PipelineRequest request) {

        PipelineResponse response =
                pipelineService.updatePipeline(pipelineId, request);
        return ResponseEntity.ok(
                ApiResponse.success("Pipeline updated successfully", response));
    }

    // DELETE /api/v1/pipelines/{pipelineId}
    @DeleteMapping("/{pipelineId}")
    @Operation(summary = "Delete a pipeline")
    public ResponseEntity<ApiResponse<Void>> deletePipeline(
            @PathVariable String pipelineId) {

        pipelineService.deletePipeline(pipelineId);
        return ResponseEntity.ok(
                ApiResponse.success("Pipeline deleted successfully", null));
    }
}