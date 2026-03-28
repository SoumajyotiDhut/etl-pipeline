package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.TransformationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test 1: Filter Transformation
 * Tests that filter correctly keeps and removes records
 * based on conditions.
 */
class FilterTransformationTest {

    // The classes we are testing
    private FilterTransformation filterTransformation;
    private ExpressionEvaluator  expressionEvaluator;

    // This runs before every single test method
    @BeforeEach
    void setUp() {
        expressionEvaluator  = new ExpressionEvaluator();
        filterTransformation = new FilterTransformation(expressionEvaluator);
    }

    // ── Helper method to create a test record ─────────────────────
    private DataRecord createRecord(long rowNum,
                                    Object... keyValues) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            fields.put(keyValues[i].toString(), keyValues[i + 1]);
        }
        return DataRecord.builder()
                .fields(fields)
                .rowNumber(rowNum)
                .hasError(false)
                .build();
    }

    // ── TEST 1.1 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Filter keeps records matching condition")
    void testFilterKeepsMatchingRecords() {
        // ARRANGE — create 3 records with different quantities
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "product_id", "PROD_A",
                        "quantity", 5L),
                createRecord(2, "product_id", "PROD_B",
                        "quantity", 0L),   // should be removed
                createRecord(3, "product_id", "PROD_C",
                        "quantity", 3L)
        );

        Map<String, Object> config = Map.of(
                "condition", "quantity > 0");

        // ACT — apply the filter
        TransformationResult result =
                filterTransformation.apply(records, config);

        // ASSERT — only 2 records should remain
        assertTrue(result.isSuccess(),
                "Filter should succeed");
        assertEquals(2, result.getRecords().size(),
                "Should keep 2 records with quantity > 0");
        assertEquals(1, result.getRecordsFiltered(),
                "Should filter out 1 record");
    }

    // ── TEST 1.2 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Filter removes all records when none match")
    void testFilterRemovesAllWhenNoneMatch() {
        // ARRANGE
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "status", "inactive"),
                createRecord(2, "status", "inactive")
        );

        Map<String, Object> config = Map.of(
                "condition", "status == 'active'");

        // ACT
        TransformationResult result =
                filterTransformation.apply(records, config);

        // ASSERT — all records removed
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRecords().size(),
                "No records should match 'active'");
        assertEquals(2, result.getRecordsFiltered(),
                "Both records should be filtered");
    }

    // ── TEST 1.3 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Filter with AND condition works correctly")
    void testFilterWithAndCondition() {
        // ARRANGE
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "quantity", 5L,
                        "status", "active"),   // matches both
                createRecord(2, "quantity", 0L,
                        "status", "active"),   // quantity fails
                createRecord(3, "quantity", 5L,
                        "status", "inactive"), // status fails
                createRecord(4, "quantity", 3L,
                        "status", "active")    // matches both
        );

        Map<String, Object> config = Map.of(
                "condition",
                "quantity > 0 AND status == 'active'");

        // ACT
        TransformationResult result =
                filterTransformation.apply(records, config);

        // ASSERT — only records 1 and 4 should pass
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRecords().size(),
                "Only records matching both conditions should pass");
    }

    // ── TEST 1.4 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Filter with empty condition keeps all records")
    void testFilterWithEmptyConditionKeepsAll() {
        // ARRANGE
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "id", 1L),
                createRecord(2, "id", 2L)
        );

        Map<String, Object> config = new HashMap<>();
        // No condition provided

        // ACT
        TransformationResult result =
                filterTransformation.apply(records, config);

        // ASSERT — all records kept
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRecords().size(),
                "All records should be kept with no condition");
    }

    // ── TEST 1.5 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Filter with IS NULL condition works")
    void testFilterWithIsNullCondition() {
        // ARRANGE
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "email", "test@test.com"),
                createRecord(2, "email", null),   // null email
                createRecord(3, "email", "other@test.com")
        );

        Map<String, Object> config = Map.of(
                "condition", "email IS NOT NULL");

        // ACT
        TransformationResult result =
                filterTransformation.apply(records, config);

        // ASSERT — only non-null emails kept
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRecords().size(),
                "Should keep only records where email is not null");
    }
}