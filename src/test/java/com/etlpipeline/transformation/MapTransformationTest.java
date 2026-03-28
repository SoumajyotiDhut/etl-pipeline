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
 * Test 2: Map Transformation
 * Tests column derivation, renaming, and string functions.
 */
class MapTransformationTest {

    private MapTransformation    mapTransformation;
    private ExpressionEvaluator  expressionEvaluator;

    @BeforeEach
    void setUp() {
        expressionEvaluator = new ExpressionEvaluator();
        mapTransformation   = new MapTransformation(expressionEvaluator);
    }

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

    // ── TEST 2.1 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Map calculates total_amount from quantity * price")
    void testMapCalculatesTotalAmount() {
        // ARRANGE
        List<DataRecord> records = List.of(
                createRecord(1,
                        "quantity", new BigDecimal("5"),
                        "price",    new BigDecimal("100"))
        );

        Map<String, Object> config = Map.of(
                "operations", Map.of(
                        "total_amount", "quantity * price"
                )
        );

        // ACT
        TransformationResult result =
                mapTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRecords().size());

        Object totalAmount = result.getRecords()
                .get(0).getFields().get("total_amount");

        assertNotNull(totalAmount,
                "total_amount field should be created");

        // 5 * 100 = 500
        assertEquals(0,
                new BigDecimal("500").compareTo(
                        new BigDecimal(totalAmount.toString())),
                "total_amount should be 500");
    }

    // ── TEST 2.2 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Map applies UPPER function to string field")
    void testMapUpperFunction() {
        // ARRANGE
        List<DataRecord> records = List.of(
                createRecord(1, "product_id", "prod_a"),
                createRecord(2, "product_id", "prod_b")
        );

        Map<String, Object> config = Map.of(
                "operations", Map.of(
                        "product_id", "UPPER(product_id)"
                )
        );

        // ACT
        TransformationResult result =
                mapTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals("PROD_A",
                result.getRecords().get(0)
                        .getFields().get("product_id"),
                "product_id should be uppercased to PROD_A");
        assertEquals("PROD_B",
                result.getRecords().get(1)
                        .getFields().get("product_id"),
                "product_id should be uppercased to PROD_B");
    }

    // ── TEST 2.3 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Map applies CONCAT to combine fields")
    void testMapConcatFunction() {
        // ARRANGE
        List<DataRecord> records = List.of(
                createRecord(1,
                        "first_name", "John",
                        "last_name",  "Doe")
        );

        Map<String, Object> config = Map.of(
                "operations", Map.of(
                        "full_name", "CONCAT(first_name, ' ', last_name)"
                )
        );

        // ACT
        TransformationResult result =
                mapTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals("John Doe",
                result.getRecords().get(0)
                        .getFields().get("full_name"),
                "full_name should be 'John Doe'");
    }

    // ── TEST 2.4 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Map preserves existing fields not in operations")
    void testMapPreservesExistingFields() {
        // ARRANGE
        List<DataRecord> records = List.of(
                createRecord(1,
                        "id",       1L,
                        "name",     "Alice",
                        "quantity", new BigDecimal("3"))
        );

        Map<String, Object> config = Map.of(
                "operations", Map.of(
                        "name", "UPPER(name)"
                )
        );

        // ACT
        TransformationResult result =
                mapTransformation.apply(records, config);

        // ASSERT — id and quantity should still be there
        assertTrue(result.isSuccess());
        Map<String, Object> fields =
                result.getRecords().get(0).getFields();

        assertNotNull(fields.get("id"),
                "id field should still exist");
        assertNotNull(fields.get("quantity"),
                "quantity field should still exist");
        assertEquals("ALICE", fields.get("name"),
                "name should be uppercased");
    }

    // ── TEST 2.5 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Map applies LOWER function correctly")
    void testMapLowerFunction() {
        // ARRANGE
        List<DataRecord> records = List.of(
                createRecord(1, "email", "TEST@EXAMPLE.COM")
        );

        Map<String, Object> config = Map.of(
                "operations", Map.of(
                        "email", "LOWER(email)"
                )
        );

        // ACT
        TransformationResult result =
                mapTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals("test@example.com",
                result.getRecords().get(0).getFields().get("email"),
                "Email should be lowercased");
    }
}
