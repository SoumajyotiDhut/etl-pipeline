package com.etlpipeline.execution;

import com.etlpipeline.destination.DestinationRouter;
import com.etlpipeline.dto.JobRunResponse;
import com.etlpipeline.exception.ResourceNotFoundException;
import com.etlpipeline.ingestion.CsvIngestionService;
import com.etlpipeline.ingestion.JsonIngestionService;
import com.etlpipeline.model.*;
import com.etlpipeline.repository.JobRunRepository;
import com.etlpipeline.repository.PipelineRepository;
import com.etlpipeline.transformation.TransformationChainResult;
import com.etlpipeline.transformation.TransformationEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class PipelineExecutionService {

    private final PipelineRepository   pipelineRepository;
    private final JobRunRepository     jobRunRepository;
    private final CsvIngestionService  csvIngestionService;
    private final JsonIngestionService jsonIngestionService;
    private final TransformationEngine transformationEngine;
    private final DestinationRouter    destinationRouter;

    // ── TRIGGER EXECUTION ─────────────────────────────────────────

    /**
     * Trigger a pipeline run.
     * Creates a JobRun record immediately and runs
     * the pipeline asynchronously in the background.
     *
     * @param pipelineId The pipeline to execute
     * @return The JobRun ID so the caller can poll for status
     */
    public JobRunResponse triggerExecution(String pipelineId) {
        log.info("Triggering execution for pipeline: {}", pipelineId);

        // Verify pipeline exists
        Pipeline pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Pipeline not found: " + pipelineId));

        // Create a JobRun with PENDING status
        String jobRunId = UUID.randomUUID().toString();

        JobRun jobRun = JobRun.builder()
                .id(jobRunId)
                .pipelineId(pipelineId)
                .tenantId(pipeline.getTenantId())
                .status(JobStatus.PENDING.name())
                .recordsRead(0L)
                .recordsWritten(0L)
                .recordsFiltered(0L)
                .recordsFailed(0L)
                .build();

        jobRunRepository.save(jobRun);
        log.info("Created JobRun: {} with status PENDING", jobRunId);

        // Run the pipeline asynchronously
        executeAsync(jobRunId, pipeline);

        return mapToResponse(jobRun);
    }

    // ── ASYNC EXECUTION ───────────────────────────────────────────

    /**
     * The actual pipeline execution runs here asynchronously.
     * This means the API call returns immediately with PENDING
     * and the pipeline runs in the background.
     */
    @Async
    @SuppressWarnings("unchecked")
    public void executeAsync(String jobRunId, Pipeline pipeline) {
        log.info("Starting async execution of JobRun: {}", jobRunId);

        // Mark as RUNNING
        updateJobStatus(jobRunId, JobStatus.RUNNING, null);
        LocalDateTime startTime = LocalDateTime.now();

        try {
            Map<String, Object> config = pipeline.getConfig();

            // Extract pipeline parts from config
            Map<String, Object> sourceConfig =
                    (Map<String, Object>) config.get("source");
            List<Map<String, Object>> transformations =
                    (List<Map<String, Object>>) config
                            .getOrDefault("transformations", List.of());
            Map<String, Object> destinationConfig =
                    (Map<String, Object>) config.get("destination");

            // ── STEP 1: INGEST ────────────────────────────────────
            log.info("[{}] Step 1: Ingesting data...", jobRunId);
            IngestionResult ingestionResult =
                    ingestData(sourceConfig);

            if (!ingestionResult.isSuccess()) {
                failJobRun(jobRunId, startTime,
                        "Ingestion failed: "
                                + ingestionResult.getGeneralError(),
                        0L, 0L, 0L, 0L);
                return;
            }

            long recordsRead = ingestionResult.getSuccessfulRows();
            log.info("[{}] Ingested {} records", jobRunId, recordsRead);

            // ── STEP 2: TRANSFORM ─────────────────────────────────
            log.info("[{}] Step 2: Applying transformations...",
                    jobRunId);
            TransformationChainResult transformResult =
                    transformationEngine.applyAll(
                            ingestionResult.getRecords(), transformations);

            if (!transformResult.isSuccess()) {
                failJobRun(jobRunId, startTime,
                        "Transformation failed: "
                                + transformResult.getErrorMessage(),
                        recordsRead,
                        0L,
                        transformResult.getTotalFiltered(),
                        transformResult.getTotalFailed());
                return;
            }

            log.info("[{}] After transforms: {} records remaining",
                    jobRunId, transformResult.getFinalRecords().size());

            // ── STEP 3: LOAD ──────────────────────────────────────
            log.info("[{}] Step 3: Writing to destination...", jobRunId);
            WriteResult writeResult = destinationRouter.route(
                    transformResult.getFinalRecords(), destinationConfig);

            if (!writeResult.isSuccess()) {
                failJobRun(jobRunId, startTime,
                        "Destination write failed: "
                                + writeResult.getErrorMessage(),
                        recordsRead,
                        0L,
                        transformResult.getTotalFiltered(),
                        transformResult.getTotalFailed());
                return;
            }

            // ── STEP 4: MARK SUCCESS ──────────────────────────────
            LocalDateTime endTime = LocalDateTime.now();
            long duration = ChronoUnit.SECONDS.between(
                    startTime, endTime);

            JobRun jobRun = jobRunRepository.findById(jobRunId)
                    .orElseThrow();
            jobRun.setStatus(JobStatus.SUCCESS.name());
            jobRun.setStartTime(startTime);
            jobRun.setEndTime(endTime);
            jobRun.setDurationSeconds(duration);
            jobRun.setRecordsRead(recordsRead);
            jobRun.setRecordsWritten(writeResult.getRecordsWritten());
            jobRun.setRecordsFiltered(transformResult.getTotalFiltered());
            jobRun.setRecordsFailed(
                    ingestionResult.getFailedRows()
                            + transformResult.getTotalFailed()
                            + writeResult.getRecordsFailed());
            jobRunRepository.save(jobRun);

            log.info("[{}] Pipeline completed successfully. "
                            + "Read: {}, Written: {}, Filtered: {}, Duration: {}s",
                    jobRunId,
                    recordsRead,
                    writeResult.getRecordsWritten(),
                    transformResult.getTotalFiltered(),
                    duration);

        } catch (Exception e) {
            log.error("[{}] Pipeline execution error: {}",
                    jobRunId, e.getMessage(), e);
            failJobRun(jobRunId, LocalDateTime.now(),
                    "Unexpected error: " + e.getMessage(),
                    0L, 0L, 0L, 0L);
        }
    }

    // ── STATUS QUERIES ────────────────────────────────────────────

    /**
     * Get the current status of a job run.
     */
    public JobRunResponse getJobRun(String jobRunId) {
        JobRun jobRun = jobRunRepository.findById(jobRunId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "JobRun not found: " + jobRunId));
        return mapToResponse(jobRun);
    }

    /**
     * Get all job runs for a pipeline.
     */
    public List<JobRunResponse> getJobRunsForPipeline(
            String pipelineId) {
        return jobRunRepository
                .findByPipelineIdOrderByCreatedAtDesc(pipelineId)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    /**
     * Get all job runs for a tenant.
     */
    public List<JobRunResponse> getJobRunsForTenant(String tenantId) {
        return jobRunRepository
                .findByTenantIdOrderByCreatedAtDesc(tenantId)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    /**
     * Cancel a running job.
     */
    public JobRunResponse cancelJobRun(String jobRunId) {
        JobRun jobRun = jobRunRepository.findById(jobRunId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "JobRun not found: " + jobRunId));

        if (JobStatus.RUNNING.name().equals(jobRun.getStatus())
                || JobStatus.PENDING.name().equals(jobRun.getStatus())) {
            jobRun.setStatus(JobStatus.CANCELLED.name());
            jobRun.setEndTime(LocalDateTime.now());
            jobRunRepository.save(jobRun);
            log.info("JobRun {} cancelled", jobRunId);
        }

        return mapToResponse(jobRun);
    }

    // ── PRIVATE HELPERS ───────────────────────────────────────────

    /**
     * Route to the correct ingestion service based on source type.
     */
    @SuppressWarnings("unchecked")
    private IngestionResult ingestData(
            Map<String, Object> sourceConfig) {
        String type = (String) sourceConfig.get("type");
        Map<String, Object> config =
                (Map<String, Object>) sourceConfig.get("config");

        if (config == null) config = sourceConfig;

        switch (type.toLowerCase()) {
            case "csv":
                return csvIngestionService.ingest(config);
            case "json":
                return jsonIngestionService.ingest(config);
            default:
                return IngestionResult.failed(
                        "Unsupported source type: " + type);
        }
    }

    private void updateJobStatus(String jobRunId,
                                 JobStatus status,
                                 String errorMessage) {
        jobRunRepository.findById(jobRunId).ifPresent(jobRun -> {
            jobRun.setStatus(status.name());
            if (status == JobStatus.RUNNING) {
                jobRun.setStartTime(LocalDateTime.now());
            }
            if (errorMessage != null) {
                jobRun.setErrorMessage(errorMessage);
            }
            jobRunRepository.save(jobRun);
        });
    }

    private void failJobRun(String jobRunId,
                            LocalDateTime startTime,
                            String errorMessage,
                            long recordsRead,
                            long recordsWritten,
                            long recordsFiltered,
                            long recordsFailed) {
        LocalDateTime endTime = LocalDateTime.now();
        long duration = ChronoUnit.SECONDS.between(startTime, endTime);

        jobRunRepository.findById(jobRunId).ifPresent(jobRun -> {
            jobRun.setStatus(JobStatus.FAILED.name());
            jobRun.setStartTime(startTime);
            jobRun.setEndTime(endTime);
            jobRun.setDurationSeconds(duration);
            jobRun.setErrorMessage(errorMessage);
            jobRun.setRecordsRead(recordsRead);
            jobRun.setRecordsWritten(recordsWritten);
            jobRun.setRecordsFiltered(recordsFiltered);
            jobRun.setRecordsFailed(recordsFailed);
            jobRunRepository.save(jobRun);
        });

        log.error("[{}] Pipeline FAILED: {}", jobRunId, errorMessage);
    }

    private JobRunResponse mapToResponse(JobRun jobRun) {
        return JobRunResponse.builder()
                .jobRunId(jobRun.getId())
                .pipelineId(jobRun.getPipelineId())
                .tenantId(jobRun.getTenantId())
                .status(jobRun.getStatus())
                .startTime(jobRun.getStartTime())
                .endTime(jobRun.getEndTime())
                .durationSeconds(jobRun.getDurationSeconds())
                .recordsRead(jobRun.getRecordsRead())
                .recordsWritten(jobRun.getRecordsWritten())
                .recordsFiltered(jobRun.getRecordsFiltered())
                .recordsFailed(jobRun.getRecordsFailed())
                .errorMessage(jobRun.getErrorMessage())
                .createdAt(jobRun.getCreatedAt())
                .build();
    }
}