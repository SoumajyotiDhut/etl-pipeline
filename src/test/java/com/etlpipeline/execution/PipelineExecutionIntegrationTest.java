package com.etlpipeline.execution;

import com.etlpipeline.ingestion.CsvIngestionService;
import com.etlpipeline.ingestion.SchemaInferenceService;
import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.IngestionResult;
import com.etlpipeline.transformation.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test 5: End-to-End Pipeline Execution
 * Tests the complete flow: ingest → transform chain → verify results.
 * This is an integration test — it tests all layers working together.
 */
class PipelineExecutionIntegrationTest {

    private CsvIngestionService     csvIngestionService;
    private TransformationEngine    transformationEngine;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Wire everything together manually
        // (same as Spring does automatically in the real app)
        SchemaInferenceService schemaInference =
                new SchemaInferenceService();
        csvIngestionService = new CsvIngestionService(schemaInference);

        ExpressionEvaluator evaluator = new ExpressionEvaluator();
        FilterTransformation filter   =
                new FilterTransformation(evaluator);
        MapTransformation map         =
                new MapTransformation(evaluator);
        AggregateTransformation agg   =
                new AggregateTransformation();

        transformationEngine =
                new TransformationEngine(filter, map, agg);
    }

    private String createCsvFile(String content) throws IOException {
        File file = tempDir.resolve("sales.csv").toFile();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file.getAbsolutePath();
    }

    // ── TEST 5.1 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Full pipeline: ingest CSV then filter removes " +
            "invalid rows")
    void testFullPipelineFilterStep() throws IOException {
        // ARRANGE — sales data with some invalid rows
        String csvContent =
                "transaction_id,product_id,quantity,price\n" +
                        "TXN001,PROD_A,5,100.00\n"  +  // valid
                        "TXN002,PROD_B,0,250.50\n"  +  // invalid: quantity = 0
                        "TXN003,PROD_A,3,100.00\n"  +  // valid
                        "TXN004,PROD_C,-1,75.25\n"  +  // invalid: quantity < 0
                        "TXN005,PROD_B,2,250.50\n";    // valid

        String filePath = createCsvFile(csvContent);

        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put("file_path",  filePath);
        sourceConfig.put("has_header", true);

        // ACT — ingest then filter
        IngestionResult ingested =
                csvIngestionService.ingest(sourceConfig);

        List<Map<String, Object>> transformations = List.of(
                Map.of(
                        "type",   "filter",
                        "config", Map.of("condition", "quantity > 0")
                )
        );

        TransformationChainResult result =
                transformationEngine.applyAll(
                        ingested.getRecords(), transformations);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(5, ingested.getSuccessfulRows(),
                "Should ingest all 5 rows");
        assertEquals(3, result.getFinalRecords().size(),
                "Filter should keep only 3 valid rows");
        assertEquals(2, result.getTotalFiltered(),
                "Filter should remove 2 invalid rows");
    }

    // ── TEST 5.2 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Full pipeline: filter → map → aggregate " +
            "produces correct results")
    void testFullPipelineThreeSteps() throws IOException {
        // ARRANGE
        String csvContent =
                "product_id,quantity,price\n" +
                        "PROD_A,5,100.00\n"  +
                        "PROD_B,3,250.00\n"  +
                        "PROD_A,0,100.00\n"  +  // filtered out
                        "PROD_A,8,100.00\n"  +
                        "PROD_B,2,250.00\n";

        String filePath = createCsvFile(csvContent);

        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put("file_path",  filePath);
        sourceConfig.put("has_header", true);

        // ACT
        IngestionResult ingested =
                csvIngestionService.ingest(sourceConfig);

        List<Map<String, Object>> transformations = List.of(
                // Step 1: filter out zero/negative quantity
                Map.of(
                        "type",   "filter",
                        "config", Map.of("condition", "quantity > 0")
                ),
                // Step 2: calculate total_amount
                Map.of(
                        "type",   "map",
                        "config", Map.of(
                                "operations", Map.of(
                                        "total_amount", "quantity * price"
                                )
                        )
                ),
                // Step 3: aggregate by product
                Map.of(
                        "type",   "aggregate",
                        "config", Map.of(
                                "group_by",     List.of("product_id"),
                                "aggregations", Map.of(
                                        "total_quantity", "SUM(quantity)",
                                        "order_count",    "COUNT()"
                                )
                        )
                )
        );

        TransformationChainResult result =
                transformationEngine.applyAll(
                        ingested.getRecords(), transformations);

        // ASSERT
        assertTrue(result.isSuccess(),
                "Pipeline should succeed");
        assertEquals(3, result.getStepsApplied(),
                "All 3 transformation steps should be applied");
        assertEquals(2, result.getFinalRecords().size(),
                "Should produce 2 groups: PROD_A and PROD_B");

        // Find PROD_A result
        Optional<DataRecord> prodA = result.getFinalRecords()
                .stream()
                .filter(r -> "PROD_A".equals(
                        r.getFields().get("product_id")))
                .findFirst();

        assertTrue(prodA.isPresent(),
                "PROD_A group should exist");

        // PROD_A has quantities 5 and 8 = 13 total (0 was filtered)
        Object totalQty = prodA.get().getFields()
                .get("total_quantity");
        assertEquals(0,
                new BigDecimal("13").compareTo(
                        new BigDecimal(totalQty.toString())),
                "PROD_A total_quantity should be 13 (5 + 8)");

        // PROD_A has 2 valid transactions
        assertEquals(2L,
                prodA.get().getFields().get("order_count"),
                "PROD_A should have 2 orders");
    }

    // ── TEST 5.3 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Pipeline with empty transformations returns " +
            "all ingested records unchanged")
    void testPipelineWithNoTransformations() throws IOException {
        // ARRANGE
        String csvContent =
                "id,value\n" +
                        "1,100\n"   +
                        "2,200\n"   +
                        "3,300\n";

        String filePath = createCsvFile(csvContent);

        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put("file_path",  filePath);
        sourceConfig.put("has_header", true);

        // ACT
        IngestionResult ingested =
                csvIngestionService.ingest(sourceConfig);

        // Empty transformation list
        TransformationChainResult result =
                transformationEngine.applyAll(
                        ingested.getRecords(), List.of());

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(3, result.getFinalRecords().size(),
                "All records should pass through unchanged");
        assertEquals(0, result.getStepsApplied(),
                "Zero steps should be applied");
        assertEquals(0, result.getTotalFiltered(),
                "Nothing should be filtered");
    }
}