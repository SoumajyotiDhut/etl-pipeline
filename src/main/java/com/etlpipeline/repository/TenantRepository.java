package com.etlpipeline.repository;

import com.etlpipeline.model.Tenant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TenantRepository extends JpaRepository<Tenant, String> {
    // JpaRepository gives you: save, findById, findAll, deleteById automatically
}
