package com.etlpipeline.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
public class PipelineResponse {

    private String pipelineId;
    private String pipelineName;
    private String tenantId;
    private String description;
    private Map<String, Object> source;
    private java.util.List<Map<String, Object>> transformations;
    private Map<String, Object> destination;
    private Map<String, Object> execution;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}