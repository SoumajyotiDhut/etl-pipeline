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
 * Test 3: Aggregate Transformation
 * Tests grouping and aggregation functions.
 */
class AggregateTransformationTest {

    private AggregateTransformation aggregateTransformation;

    @BeforeEach
    void setUp() {
        aggregateTransformation = new AggregateTransformation();
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

    // ── TEST 3.1 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Aggregate groups records by product_id correctly")
    void testAggregateGroupsByProductId() {
        // ARRANGE — 4 records, 2 products
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "product_id", "PROD_A",
                        "quantity",   new BigDecimal("5")),
                createRecord(2, "product_id", "PROD_B",
                        "quantity",   new BigDecimal("3")),
                createRecord(3, "product_id", "PROD_A",
                        "quantity",   new BigDecimal("8")),
                createRecord(4, "product_id", "PROD_B",
                        "quantity",   new BigDecimal("2"))
        );

        Map<String, Object> config = Map.of(
                "group_by",     List.of("product_id"),
                "aggregations", Map.of(
                        "total_quantity", "SUM(quantity)",
                        "order_count",    "COUNT()"
                )
        );

        // ACT
        TransformationResult result =
                aggregateTransformation.apply(records, config);

        // ASSERT — should produce 2 groups
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRecords().size(),
                "Should produce 2 groups — one per product");
    }

    // ── TEST 3.2 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Aggregate SUM calculates correct total")
    void testAggregateSumIsCorrect() {
        // ARRANGE — PROD_A has quantities 5 and 8 = 13 total
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "product_id", "PROD_A",
                        "quantity",   new BigDecimal("5")),
                createRecord(2, "product_id", "PROD_A",
                        "quantity",   new BigDecimal("8"))
        );

        Map<String, Object> config = Map.of(
                "group_by",     List.of("product_id"),
                "aggregations", Map.of(
                        "total_quantity", "SUM(quantity)"
                )
        );

        // ACT
        TransformationResult result =
                aggregateTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRecords().size());

        Object totalQty = result.getRecords()
                .get(0).getFields().get("total_quantity");

        assertNotNull(totalQty, "total_quantity should exist");
        assertEquals(0,
                new BigDecimal("13").compareTo(
                        new BigDecimal(totalQty.toString())),
                "SUM of quantities should be 13");
    }

    // ── TEST 3.3 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Aggregate COUNT returns correct count")
    void testAggregateCountIsCorrect() {
        // ARRANGE — 3 records all in same group
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "region", "NORTH",
                        "sales",  new BigDecimal("100")),
                createRecord(2, "region", "NORTH",
                        "sales",  new BigDecimal("200")),
                createRecord(3, "region", "NORTH",
                        "sales",  new BigDecimal("150"))
        );

        Map<String, Object> config = Map.of(
                "group_by",     List.of("region"),
                "aggregations", Map.of(
                        "order_count", "COUNT()"
                )
        );

        // ACT
        TransformationResult result =
                aggregateTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        Object count = result.getRecords()
                .get(0).getFields().get("order_count");

        assertEquals(3L, count,
                "COUNT should return 3");
    }

    // ── TEST 3.4 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Aggregate AVG calculates correct average")
    void testAggregateAvgIsCorrect() {
        // ARRANGE — prices: 100, 200, 300 → avg = 200
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "category", "A",
                        "price",    new BigDecimal("100")),
                createRecord(2, "category", "A",
                        "price",    new BigDecimal("200")),
                createRecord(3, "category", "A",
                        "price",    new BigDecimal("300"))
        );

        Map<String, Object> config = Map.of(
                "group_by",     List.of("category"),
                "aggregations", Map.of(
                        "avg_price", "AVG(price)"
                )
        );

        // ACT
        TransformationResult result =
                aggregateTransformation.apply(records, config);

        // ASSERT
        assertTrue(result.isSuccess());
        Object avgPrice = result.getRecords()
                .get(0).getFields().get("avg_price");

        assertNotNull(avgPrice);
        assertEquals(0,
                new BigDecimal("200").compareTo(
                        new BigDecimal(avgPrice.toString())),
                "AVG of 100+200+300 should be 200");
    }

    // ── TEST 3.5 ──────────────────────────────────────────────────
    @Test
    @DisplayName("Aggregate with no group_by treats all records as one group")
    void testAggregateWithNoGroupBy() {
        // ARRANGE
        List<DataRecord> records = Arrays.asList(
                createRecord(1, "amount", new BigDecimal("50")),
                createRecord(2, "amount", new BigDecimal("75")),
                createRecord(3, "amount", new BigDecimal("25"))
        );

        Map<String, Object> config = Map.of(
                "aggregations", Map.of(
                        "total", "SUM(amount)",
                        "count", "COUNT()"
                )
        );

        // ACT
        TransformationResult result =
                aggregateTransformation.apply(records, config);

        // ASSERT — one group for all records
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRecords().size(),
                "No group_by = all records in one group");

        Object total = result.getRecords()
                .get(0).getFields().get("total");
        assertEquals(0,
                new BigDecimal("150").compareTo(
                        new BigDecimal(total.toString())),
                "Total should be 150");
    }
}