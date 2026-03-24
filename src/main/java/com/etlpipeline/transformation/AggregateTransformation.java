package com.etlpipeline.transformation;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.TransformationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Component
@Slf4j
public class AggregateTransformation {

    /**
     * Aggregate transformation — group records and compute aggregates.
     *
     * Config example:
     * {
     *   "group_by": ["product_id", "region"],
     *   "aggregations": {
     *     "total_quantity": "SUM(quantity)",
     *     "avg_price":      "AVG(price)",
     *     "order_count":    "COUNT()",
     *     "max_amount":     "MAX(amount)",
     *     "min_amount":     "MIN(amount)"
     *   }
     * }
     */
    @SuppressWarnings("unchecked")
    public TransformationResult apply(List<DataRecord> records,
                                      Map<String, Object> config) {
        List<String> groupByFields =
                (List<String>) config.get("group_by");
        Map<String, String> aggregations =
                (Map<String, String>) config.get("aggregations");

        if (aggregations == null || aggregations.isEmpty()) {
            log.warn("Aggregate has no aggregations defined");
            return TransformationResult.builder()
                    .records(records)
                    .recordsFiltered(0)
                    .recordsFailed(0)
                    .transformationType("aggregate")
                    .success(true)
                    .build();
        }

        if (groupByFields == null) groupByFields = new ArrayList<>();

        log.info("Applying aggregate. Group by: {}, Aggregations: {}",
                groupByFields, aggregations.keySet());

        // Step 1: Group records by the group_by key
        Map<String, List<DataRecord>> groups = new LinkedHashMap<>();

        for (DataRecord record : records) {
            if (record.isHasError()) continue;

            String groupKey = buildGroupKey(record, groupByFields);
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>())
                    .add(record);
        }

        // Step 2: For each group compute aggregations
        List<DataRecord> result = new ArrayList<>();
        long rowNum = 0;

        for (Map.Entry<String, List<DataRecord>> entry : groups.entrySet()) {
            List<DataRecord> group = entry.getValue();
            rowNum++;

            Map<String, Object> aggregatedFields = new LinkedHashMap<>();

            // Add group_by field values from the first record in the group
            for (String field : groupByFields) {
                Object val = group.get(0).getFields().get(field);
                aggregatedFields.put(field, val);
            }

            // Compute each aggregation
            for (Map.Entry<String, String> agg : aggregations.entrySet()) {
                String outputField = agg.getKey();
                String expression  = agg.getValue().trim();

                Object aggregatedValue = computeAggregation(
                        group, expression);
                aggregatedFields.put(outputField, aggregatedValue);
            }

            result.add(DataRecord.builder()
                    .fields(aggregatedFields)
                    .rowNumber(rowNum)
                    .hasError(false)
                    .build());
        }

        log.info("Aggregate complete. Groups: {}", result.size());

        return TransformationResult.builder()
                .records(result)
                .recordsFiltered(records.size() - result.size())
                .recordsFailed(0)
                .transformationType("aggregate")
                .success(true)
                .build();
    }

    // ── Private helpers ───────────────────────────────────────────

    private String buildGroupKey(DataRecord record,
                                 List<String> groupByFields) {
        if (groupByFields.isEmpty()) return "__all__";

        StringBuilder key = new StringBuilder();
        for (String field : groupByFields) {
            Object val = record.getFields().get(field);
            key.append(val != null ? val.toString() : "null");
            key.append("|");
        }
        return key.toString();
    }

    private Object computeAggregation(List<DataRecord> group,
                                      String expression) {
        String upper = expression.toUpperCase();

        // COUNT()
        if (upper.startsWith("COUNT")) {
            return (long) group.size();
        }

        // Extract field name from SUM(field), AVG(field), etc.
        String fieldName = extractFieldName(expression);

        if (upper.startsWith("SUM(")) {
            BigDecimal sum = BigDecimal.ZERO;
            for (DataRecord r : group) {
                BigDecimal val = toBigDecimal(r.getFields().get(fieldName));
                if (val != null) sum = sum.add(val);
            }
            return sum;
        }

        if (upper.startsWith("AVG(")) {
            BigDecimal sum = BigDecimal.ZERO;
            int count = 0;
            for (DataRecord r : group) {
                BigDecimal val = toBigDecimal(r.getFields().get(fieldName));
                if (val != null) { sum = sum.add(val); count++; }
            }
            return count > 0
                    ? sum.divide(BigDecimal.valueOf(count), 4,
                    RoundingMode.HALF_UP)
                    : BigDecimal.ZERO;
        }

        if (upper.startsWith("MAX(")) {
            BigDecimal max = null;
            for (DataRecord r : group) {
                BigDecimal val = toBigDecimal(r.getFields().get(fieldName));
                if (val != null && (max == null || val.compareTo(max) > 0)) {
                    max = val;
                }
            }
            return max;
        }

        if (upper.startsWith("MIN(")) {
            BigDecimal min = null;
            for (DataRecord r : group) {
                BigDecimal val = toBigDecimal(r.getFields().get(fieldName));
                if (val != null && (min == null || val.compareTo(min) < 0)) {
                    min = val;
                }
            }
            return min;
        }

        if (upper.startsWith("FIRST(")) {
            return group.isEmpty() ? null
                    : group.get(0).getFields().get(fieldName);
        }

        if (upper.startsWith("LAST(")) {
            return group.isEmpty() ? null
                    : group.get(group.size() - 1).getFields().get(fieldName);
        }

        return null;
    }

    private String extractFieldName(String expression) {
        int start = expression.indexOf('(');
        int end   = expression.lastIndexOf(')');
        if (start < 0 || end < 0) return expression;
        return expression.substring(start + 1, end).trim();
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        try { return new BigDecimal(value.toString()); }
        catch (Exception e) { return null; }
    }
}