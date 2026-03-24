package com.etlpipeline.repository;

import com.etlpipeline.model.Pipeline;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PipelineRepository extends JpaRepository<Pipeline, String> {

    // Find all pipelines belonging to a specific tenant
    List<Pipeline> findByTenantId(String tenantId);

    // Check if a pipeline name already exists for a tenant
    boolean existsByPipelineNameAndTenantId(String pipelineName, String tenantId);
}