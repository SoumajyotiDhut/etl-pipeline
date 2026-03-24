package com.etlpipeline.repository;

import com.etlpipeline.model.JobRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRunRepository extends JpaRepository<JobRun, String> {

    // Find all runs for a specific pipeline
    List<JobRun> findByPipelineIdOrderByCreatedAtDesc(String pipelineId);

    // Find all runs for a tenant
    List<JobRun> findByTenantIdOrderByCreatedAtDesc(String tenantId);

    // Find runs by status
    List<JobRun> findByStatus(String status);
}