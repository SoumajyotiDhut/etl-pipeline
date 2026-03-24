package com.etlpipeline.model;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class TransformationResult {

    // Records after transformation
    private List<DataRecord> records;

    // How many records were removed by filters
    private long recordsFiltered;

    // How many records had errors during transformation
    private long recordsFailed;

    // Which transformation type produced this result
    private String transformationType;

    // Whether transformation succeeded
    private boolean success;

    // Error message if something went wrong
    private String errorMessage;

    public static TransformationResult failed(String errorMessage) {
        return TransformationResult.builder()
                .success(false)
                .errorMessage(errorMessage)
                .recordsFiltered(0)
                .recordsFailed(0)
                .build();
    }
}