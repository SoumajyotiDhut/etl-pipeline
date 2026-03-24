package com.etlpipeline.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "job_runs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobRun {

    @Id
    @Column(name = "id", nullable = false)
    private String id;

    @Column(name = "pipeline_id", nullable = false)
    private String pipelineId;

    @Column(name = "tenant_id", nullable = false)
    private String tenantId;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "start_time")
    private LocalDateTime startTime;

    @Column(name = "end_time")
    private LocalDateTime endTime;

    @Column(name = "duration_seconds")
    private Long durationSeconds;

    @Column(name = "records_read")
    private Long recordsRead;

    @Column(name = "records_written")
    private Long recordsWritten;

    @Column(name = "records_filtered")
    private Long recordsFiltered;

    @Column(name = "records_failed")
    private Long recordsFailed;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        // Set default values
        if (recordsRead == null) recordsRead = 0L;
        if (recordsWritten == null) recordsWritten = 0L;
        if (recordsFiltered == null) recordsFiltered = 0L;
        if (recordsFailed == null) recordsFailed = 0L;
    }
}