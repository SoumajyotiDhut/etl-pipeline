package com.etlpipeline.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WriteResult {

    private long recordsWritten;
    private long recordsFailed;
    private boolean success;
    private String errorMessage;
    private String destinationType;

    // For file output — where the file was written
    private String outputPath;

    public static WriteResult failed(String errorMessage) {
        return WriteResult.builder()
                .success(false)
                .errorMessage(errorMessage)
                .recordsWritten(0)
                .recordsFailed(0)
                .build();
    }
}
