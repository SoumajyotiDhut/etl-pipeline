package com.etlpipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
public class PipelineRequest {

    @NotBlank(message = "Pipeline ID is required")
    private String pipelineId;

    @NotBlank(message = "Pipeline name is required")
    private String pipelineName;

    @NotBlank(message = "Tenant ID is required")
    private String tenantId;

    private String description;

    @NotNull(message = "Source configuration is required")
    private Map<String, Object> source;

    private java.util.List<Map<String, Object>> transformations;

    @NotNull(message = "Destination configuration is required")
    private Map<String, Object> destination;

    private Map<String, Object> execution;
}
