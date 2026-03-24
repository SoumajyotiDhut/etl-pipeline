package com.etlpipeline.service;

import com.etlpipeline.dto.PipelineRequest;
import com.etlpipeline.dto.PipelineResponse;
import com.etlpipeline.exception.ResourceNotFoundException;
import com.etlpipeline.exception.ValidationException;
import com.etlpipeline.model.Pipeline;
import com.etlpipeline.repository.PipelineRepository;
import com.etlpipeline.repository.TenantRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PipelineService {

    private final PipelineRepository pipelineRepository;
    private final TenantRepository tenantRepository;

    // ─── CREATE ────────────────────────────────────────────────
    @Transactional
    public PipelineResponse createPipeline(PipelineRequest request) {
        log.info("Creating pipeline: {} for tenant: {}",
                request.getPipelineId(), request.getTenantId());

        // Check tenant exists
        if (!tenantRepository.existsById(request.getTenantId())) {
            throw new ValidationException(
                    "Tenant not found: " + request.getTenantId());
        }

        // Check pipeline ID is not already taken
        if (pipelineRepository.existsById(request.getPipelineId())) {
            throw new ValidationException(
                    "Pipeline with ID already exists: " + request.getPipelineId());
        }

        // Validate source type
        validateSourceType(request.getSource());

        // Build the config map that goes into the JSONB column
        Map<String, Object> config = buildConfigMap(request);

        // Build and save the pipeline entity
        Pipeline pipeline = Pipeline.builder()
                .id(request.getPipelineId())
                .pipelineName(request.getPipelineName())
                .tenantId(request.getTenantId())
                .description(request.getDescription())
                .config(config)
                .build();

        Pipeline saved = pipelineRepository.save(pipeline);
        log.info("Pipeline created successfully: {}", saved.getId());

        return mapToResponse(saved);
    }

    // ─── READ ONE ───────────────────────────────────────────────
    public PipelineResponse getPipeline(String pipelineId) {
        Pipeline pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Pipeline not found: " + pipelineId));
        return mapToResponse(pipeline);
    }

    // ─── READ ALL (by tenant) ───────────────────────────────────
    public List<PipelineResponse> getAllPipelines(String tenantId) {
        List<Pipeline> pipelines;

        if (tenantId != null && !tenantId.isEmpty()) {
            pipelines = pipelineRepository.findByTenantId(tenantId);
        } else {
            pipelines = pipelineRepository.findAll();
        }

        return pipelines.stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    // ─── UPDATE ────────────────────────────────────────────────
    @Transactional
    public PipelineResponse updatePipeline(String pipelineId,
                                           PipelineRequest request) {
        log.info("Updating pipeline: {}", pipelineId);

        Pipeline existing = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Pipeline not found: " + pipelineId));

        // Validate source type if source is provided
        if (request.getSource() != null) {
            validateSourceType(request.getSource());
        }

        // Update fields
        existing.setPipelineName(request.getPipelineName());
        existing.setDescription(request.getDescription());
        existing.setConfig(buildConfigMap(request));

        Pipeline updated = pipelineRepository.save(existing);
        log.info("Pipeline updated successfully: {}", updated.getId());

        return mapToResponse(updated);
    }

    // ─── DELETE ────────────────────────────────────────────────
    @Transactional
    public void deletePipeline(String pipelineId) {
        log.info("Deleting pipeline: {}", pipelineId);

        if (!pipelineRepository.existsById(pipelineId)) {
            throw new ResourceNotFoundException(
                    "Pipeline not found: " + pipelineId);
        }

        pipelineRepository.deleteById(pipelineId);
        log.info("Pipeline deleted successfully: {}", pipelineId);
    }

    // ─── PRIVATE HELPERS ────────────────────────────────────────

    // Validate that source type is supported
    private void validateSourceType(Map<String, Object> source) {
        if (source == null || !source.containsKey("type")) {
            throw new ValidationException("Source must have a 'type' field");
        }

        String sourceType = source.get("type").toString();
        if (!sourceType.equals("csv") && !sourceType.equals("json")) {
            throw new ValidationException(
                    "Unsupported source type: " + sourceType +
                            ". Supported types: csv, json");
        }
    }

    // Build the config map from the request
    private Map<String, Object> buildConfigMap(PipelineRequest request) {
        Map<String, Object> config = new HashMap<>();
        config.put("pipeline_id", request.getPipelineId());
        config.put("pipeline_name", request.getPipelineName());
        config.put("tenant_id", request.getTenantId());
        config.put("description", request.getDescription());
        config.put("source", request.getSource());
        config.put("transformations",
                request.getTransformations() != null ?
                        request.getTransformations() : List.of());
        config.put("destination", request.getDestination());
        if (request.getExecution() != null) {
            config.put("execution", request.getExecution());
        }
        return config;
    }

    // Convert Pipeline entity → PipelineResponse DTO
    @SuppressWarnings("unchecked")
    private PipelineResponse mapToResponse(Pipeline pipeline) {
        Map<String, Object> config = pipeline.getConfig();

        return PipelineResponse.builder()
                .pipelineId(pipeline.getId())
                .pipelineName(pipeline.getPipelineName())
                .tenantId(pipeline.getTenantId())
                .description(pipeline.getDescription())
                .source((Map<String, Object>) config.get("source"))
                .transformations((List<Map<String, Object>>)
                        config.get("transformations"))
                .destination((Map<String, Object>) config.get("destination"))
                .execution((Map<String, Object>) config.get("execution"))
                .createdAt(pipeline.getCreatedAt())
                .updatedAt(pipeline.getUpdatedAt())
                .build();
    }
}