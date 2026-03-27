package com.etlpipeline.controller;

import com.etlpipeline.dto.ApiResponse;
import com.etlpipeline.dto.JobRunResponse;
import com.etlpipeline.execution.PipelineExecutionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Pipeline Execution",
        description = "APIs for running pipelines and tracking status")
public class ExecutionController {

    private final PipelineExecutionService executionService;

    // POST /api/v1/pipelines/{pipelineId}/run
    @PostMapping("/pipelines/{pipelineId}/run")
    @Operation(summary = "Trigger a pipeline execution")
    public ResponseEntity<ApiResponse<JobRunResponse>> runPipeline(
            @PathVariable String pipelineId) {

        JobRunResponse jobRun =
                executionService.triggerExecution(pipelineId);

        return ResponseEntity.ok(
                ApiResponse.success(
                        "Pipeline execution started. "
                                + "Job Run ID: " + jobRun.getJobRunId(),
                        jobRun));
    }

    // GET /api/v1/runs/{jobRunId}
    @GetMapping("/runs/{jobRunId}")
    @Operation(summary = "Get status of a specific job run")
    public ResponseEntity<ApiResponse<JobRunResponse>> getJobRun(
            @PathVariable String jobRunId) {

        JobRunResponse jobRun = executionService.getJobRun(jobRunId);
        return ResponseEntity.ok(
                ApiResponse.success("Job run retrieved", jobRun));
    }

    // GET /api/v1/pipelines/{pipelineId}/runs
    @GetMapping("/pipelines/{pipelineId}/runs")
    @Operation(summary = "Get all runs for a pipeline")
    public ResponseEntity<ApiResponse<List<JobRunResponse>>>
    getPipelineRuns(@PathVariable String pipelineId) {

        List<JobRunResponse> runs =
                executionService.getJobRunsForPipeline(pipelineId);
        return ResponseEntity.ok(
                ApiResponse.success("Pipeline runs retrieved", runs));
    }

    // GET /api/v1/runs?tenantId=tenant_123
    @GetMapping("/runs")
    @Operation(summary = "Get all runs for a tenant")
    public ResponseEntity<ApiResponse<List<JobRunResponse>>>
    getTenantRuns(
            @RequestParam(required = false) String tenantId) {

        List<JobRunResponse> runs =
                executionService.getJobRunsForTenant(tenantId);
        return ResponseEntity.ok(
                ApiResponse.success("Tenant runs retrieved", runs));
    }

    // DELETE /api/v1/runs/{jobRunId}/cancel
    @DeleteMapping("/runs/{jobRunId}/cancel")
    @Operation(summary = "Cancel a running job")
    public ResponseEntity<ApiResponse<JobRunResponse>> cancelRun(
            @PathVariable String jobRunId) {

        JobRunResponse jobRun = executionService.cancelJobRun(jobRunId);
        return ResponseEntity.ok(
                ApiResponse.success("Job run cancelled", jobRun));
    }
}