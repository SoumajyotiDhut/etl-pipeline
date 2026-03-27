package com.etlpipeline.model;

public enum JobStatus {
    PENDING,    // Job created but not started
    RUNNING,    // Currently executing
    SUCCESS,    // Completed successfully
    FAILED,     // Execution failed
    CANCELLED   // Manually stopped
}