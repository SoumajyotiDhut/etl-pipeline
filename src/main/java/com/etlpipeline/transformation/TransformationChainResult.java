package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class TransformationChainResult {

    private List<DataRecord> finalRecords;
    private long totalFiltered;
    private long totalFailed;
    private int stepsApplied;
    private boolean success;
    private String errorMessage;
}