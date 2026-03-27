package com.etlpipeline.dto;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Builder
public class JobRunResponse {

    private String jobRunId;
    private String pipelineId;
    private String tenantId;
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long durationSeconds;
    private Long recordsRead;
    private Long recordsWritten;
    private Long recordsFiltered;
    private Long recordsFailed;
    private String errorMessage;
    private LocalDateTime createdAt;
}