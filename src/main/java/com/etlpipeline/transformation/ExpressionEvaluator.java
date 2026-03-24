package com.etlpipeline.transformation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

@Component
@Slf4j
public class ExpressionEvaluator {

    // ─────────────────────────────────────────────────────────────
    // CONDITION EVALUATION (used by Filter transformation)
    // Examples: "quantity > 0", "status == 'active'", "age >= 18"
    // ─────────────────────────────────────────────────────────────

    public boolean evaluateCondition(String condition,
                                     Map<String, Object> fields) {
        if (condition == null || condition.trim().isEmpty()) {
            return true;
        }

        try {
            condition = condition.trim();

            // Handle AND / OR (evaluate left and right sides)
            if (containsLogicalOperator(condition, " AND ")) {
                String[] parts = splitOnOperator(condition, " AND ");
                return evaluateCondition(parts[0], fields)
                        && evaluateCondition(parts[1], fields);
            }
            if (containsLogicalOperator(condition, " OR ")) {
                String[] parts = splitOnOperator(condition, " OR ");
                return evaluateCondition(parts[0], fields)
                        || evaluateCondition(parts[1], fields);
            }

            // Handle NOT
            if (condition.toUpperCase().startsWith("NOT ")) {
                String inner = condition.substring(4).trim();
                return !evaluateCondition(inner, fields);
            }

            // Remove wrapping parentheses if present
            if (condition.startsWith("(") && condition.endsWith(")")) {
                return evaluateCondition(
                        condition.substring(1, condition.length() - 1), fields);
            }

            // IS NULL / IS NOT NULL
            if (condition.toUpperCase().contains(" IS NOT NULL")) {
                String fieldName = condition.split("(?i)\\s+IS\\s+NOT\\s+NULL")[0].trim();
                return fields.get(fieldName) != null;
            }
            if (condition.toUpperCase().contains(" IS NULL")) {
                String fieldName = condition.split("(?i)\\s+IS\\s+NULL")[0].trim();
                return fields.get(fieldName) == null;
            }

            // String functions: CONTAINS, STARTS_WITH, ENDS_WITH, LIKE
            if (condition.toUpperCase().contains(" CONTAINS ")) {
                String[] parts = condition.split("(?i)\\s+CONTAINS\\s+", 2);
                String fieldVal = getStringValue(parts[0].trim(), fields);
                String searchVal = unquote(parts[1].trim());
                return fieldVal != null
                        && fieldVal.toLowerCase()
                        .contains(searchVal.toLowerCase());
            }
            if (condition.toUpperCase().contains(" STARTS_WITH ")) {
                String[] parts = condition.split("(?i)\\s+STARTS_WITH\\s+", 2);
                String fieldVal = getStringValue(parts[0].trim(), fields);
                String searchVal = unquote(parts[1].trim());
                return fieldVal != null
                        && fieldVal.toLowerCase()
                        .startsWith(searchVal.toLowerCase());
            }
            if (condition.toUpperCase().contains(" ENDS_WITH ")) {
                String[] parts = condition.split("(?i)\\s+ENDS_WITH\\s+", 2);
                String fieldVal = getStringValue(parts[0].trim(), fields);
                String searchVal = unquote(parts[1].trim());
                return fieldVal != null
                        && fieldVal.toLowerCase()
                        .endsWith(searchVal.toLowerCase());
            }
            if (condition.toUpperCase().contains(" LIKE ")) {
                String[] parts = condition.split("(?i)\\s+LIKE\\s+", 2);
                String fieldVal = getStringValue(parts[0].trim(), fields);
                String pattern  = unquote(parts[1].trim())
                        .replace("%", ".*").replace("_", ".");
                return fieldVal != null
                        && fieldVal.toLowerCase().matches(pattern.toLowerCase());
            }

            // Comparison operators: ==, !=, >=, <=, >,
            for (String op : new String[]{"==", "!=", ">=", "<=", ">", "<"}) {
                if (condition.contains(op)) {
                    String[] parts = condition.split(
                            java.util.regex.Pattern.quote(op), 2);
                    if (parts.length == 2) {
                        return evaluateComparison(
                                parts[0].trim(), op, parts[1].trim(), fields);
                    }
                }
            }

            log.warn("Could not evaluate condition: {}", condition);
            return false;

        } catch (Exception e) {
            log.warn("Error evaluating condition '{}': {}",
                    condition, e.getMessage());
            return false;
        }
    }

    // ─────────────────────────────────────────────────────────────
    // EXPRESSION EVALUATION (used by Map transformation)
    // Examples: "UPPER(email)", "quantity * price", "CONCAT(a,' ',b)"
    // ─────────────────────────────────────────────────────────────

    public Object evaluateExpression(String expression,
                                     Map<String, Object> fields) {
        if (expression == null) return null;

        expression = expression.trim();

        // If expression is just a field name, return its value
        if (fields.containsKey(expression)) {
            return fields.get(expression);
        }

        // String functions
        if (expression.toUpperCase().startsWith("UPPER(")) {
            String inner = extractFunctionArg(expression);
            Object val = resolveValue(inner, fields);
            return val != null ? val.toString().toUpperCase() : null;
        }
        if (expression.toUpperCase().startsWith("LOWER(")) {
            String inner = extractFunctionArg(expression);
            Object val = resolveValue(inner, fields);
            return val != null ? val.toString().toLowerCase() : null;
        }
        if (expression.toUpperCase().startsWith("TRIM(")) {
            String inner = extractFunctionArg(expression);
            Object val = resolveValue(inner, fields);
            return val != null ? val.toString().trim() : null;
        }
        if (expression.toUpperCase().startsWith("CONCAT(")) {
            return evaluateConcat(expression, fields);
        }
        if (expression.toUpperCase().startsWith("SUBSTRING(")) {
            return evaluateSubstring(expression, fields);
        }
        if (expression.toUpperCase().startsWith("REPLACE(")) {
            return evaluateReplace(expression, fields);
        }

        // Math functions
        if (expression.toUpperCase().startsWith("ROUND(")) {
            return evaluateRound(expression, fields);
        }
        if (expression.toUpperCase().startsWith("ABS(")) {
            String inner = extractFunctionArg(expression);
            BigDecimal val = toBigDecimal(resolveValue(inner, fields));
            return val != null ? val.abs() : null;
        }
        if (expression.toUpperCase().startsWith("CEIL(")) {
            String inner = extractFunctionArg(expression);
            BigDecimal val = toBigDecimal(resolveValue(inner, fields));
            return val != null
                    ? val.setScale(0, RoundingMode.CEILING).longValue() : null;
        }
        if (expression.toUpperCase().startsWith("FLOOR(")) {
            String inner = extractFunctionArg(expression);
            BigDecimal val = toBigDecimal(resolveValue(inner, fields));
            return val != null
                    ? val.setScale(0, RoundingMode.FLOOR).longValue() : null;
        }

        // Arithmetic: +, -, *, /
        // Try operators from lowest to highest precedence
        for (String op : new String[]{"+", "-", "*", "/"}) {
            int idx = findOperatorIndex(expression, op);
            if (idx > 0) {
                String left  = expression.substring(0, idx).trim();
                String right = expression.substring(idx + 1).trim();
                return evaluateArithmetic(left, op, right, fields);
            }
        }

        // Quoted string literal
        if ((expression.startsWith("'") && expression.endsWith("'"))
                || (expression.startsWith("\"") && expression.endsWith("\""))) {
            return unquote(expression);
        }

        // Numeric literal
        try { return Long.parseLong(expression); }
        catch (NumberFormatException ignored) {}
        try { return new BigDecimal(expression); }
        catch (NumberFormatException ignored) {}

        // Boolean literal
        if (expression.equalsIgnoreCase("true"))  return true;
        if (expression.equalsIgnoreCase("false")) return false;

        // Return as string if nothing else matches
        return expression;
    }

    // ─────────────────────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────────────────────

    private boolean evaluateComparison(String left, String op,
                                       String right,
                                       Map<String, Object> fields) {
        Object leftVal  = resolveValue(left, fields);
        Object rightVal = resolveValue(right, fields);

        // Null handling
        if (leftVal == null || rightVal == null) {
            if (op.equals("==")) return leftVal == rightVal;
            if (op.equals("!=")) return leftVal != rightVal;
            return false;
        }

        // String comparison
        if (leftVal instanceof String || rightVal instanceof String) {
            String l = leftVal.toString();
            String r = unquote(rightVal.toString());
            switch (op) {
                case "==": return l.equals(r);
                case "!=": return !l.equals(r);
                default:   return l.compareTo(r) > 0;
            }
        }

        // Numeric comparison
        BigDecimal l = toBigDecimal(leftVal);
        BigDecimal r = toBigDecimal(rightVal);
        if (l == null || r == null) return false;

        int cmp = l.compareTo(r);
        switch (op) {
            case "==": return cmp == 0;
            case "!=": return cmp != 0;
            case ">":  return cmp > 0;
            case "<":  return cmp < 0;
            case ">=": return cmp >= 0;
            case "<=": return cmp <= 0;
            default:   return false;
        }
    }

    private Object evaluateArithmetic(String left, String op,
                                      String right,
                                      Map<String, Object> fields) {
        Object leftVal  = resolveValue(left, fields);
        Object rightVal = resolveValue(right, fields);

        BigDecimal l = toBigDecimal(leftVal);
        BigDecimal r = toBigDecimal(rightVal);

        if (l == null || r == null) return null;

        switch (op) {
            case "+": return l.add(r);
            case "-": return l.subtract(r);
            case "*": return l.multiply(r);
            case "/":
                if (r.compareTo(BigDecimal.ZERO) == 0) return null;
                return l.divide(r, 10, RoundingMode.HALF_UP);
            default:  return null;
        }
    }

    private Object evaluateConcat(String expression,
                                  Map<String, Object> fields) {
        String args = extractFunctionArg(expression);
        String[] parts = args.split(",");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            Object val = resolveValue(part.trim(), fields);
            if (val != null) sb.append(val);
        }
        return sb.toString();
    }

    private Object evaluateSubstring(String expression,
                                     Map<String, Object> fields) {
        String args   = extractFunctionArg(expression);
        String[] parts = args.split(",");
        if (parts.length < 2) return null;

        Object val = resolveValue(parts[0].trim(), fields);
        if (val == null) return null;

        String str   = val.toString();
        int start    = Integer.parseInt(parts[1].trim());
        int end      = parts.length > 2
                ? Integer.parseInt(parts[2].trim()) : str.length();

        start = Math.max(0, start);
        end   = Math.min(str.length(), end);
        return start < end ? str.substring(start, end) : "";
    }

    private Object evaluateReplace(String expression,
                                   Map<String, Object> fields) {
        String args    = extractFunctionArg(expression);
        String[] parts = args.split(",");
        if (parts.length < 3) return null;

        Object val = resolveValue(parts[0].trim(), fields);
        if (val == null) return null;

        String search  = unquote(parts[1].trim());
        String replace = unquote(parts[2].trim());
        return val.toString().replace(search, replace);
    }

    private Object evaluateRound(String expression,
                                 Map<String, Object> fields) {
        String args    = extractFunctionArg(expression);
        String[] parts = args.split(",");

        Object val = resolveValue(parts[0].trim(), fields);
        BigDecimal num = toBigDecimal(val);
        if (num == null) return null;

        int scale = parts.length > 1
                ? Integer.parseInt(parts[1].trim()) : 0;
        return num.setScale(scale, RoundingMode.HALF_UP);
    }

    // Resolve a token — either a field name or a literal value
    private Object resolveValue(String token, Map<String, Object> fields) {
        token = token.trim();

        // Check if it's a field name
        if (fields.containsKey(token)) {
            return fields.get(token);
        }

        // Quoted string literal
        if ((token.startsWith("'") && token.endsWith("'"))
                || (token.startsWith("\"") && token.endsWith("\""))) {
            return unquote(token);
        }

        // Numeric literal
        try { return Long.parseLong(token); }
        catch (NumberFormatException ignored) {}
        try { return new BigDecimal(token); }
        catch (NumberFormatException ignored) {}

        // Boolean
        if (token.equalsIgnoreCase("true"))  return true;
        if (token.equalsIgnoreCase("false")) return false;

        return token;
    }

    private String getStringValue(String fieldName,
                                  Map<String, Object> fields) {
        Object val = fields.get(fieldName);
        return val != null ? val.toString() : null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        try { return new BigDecimal(value.toString()); }
        catch (Exception e) { return null; }
    }

    private String extractFunctionArg(String expression) {
        int start = expression.indexOf('(');
        int end   = expression.lastIndexOf(')');
        if (start < 0 || end < 0) return expression;
        return expression.substring(start + 1, end);
    }

    private String unquote(String value) {
        if (value == null) return null;
        value = value.trim();
        if ((value.startsWith("'") && value.endsWith("'"))
                || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    // Find operator index, skipping operators inside parentheses
    private int findOperatorIndex(String expression, String op) {
        int depth = 0;
        for (int i = 0; i < expression.length() - op.length() + 1; i++) {
            char c = expression.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0
                    && expression.substring(i).startsWith(op)) {
                return i;
            }
        }
        return -1;
    }

    private boolean containsLogicalOperator(String condition, String op) {
        int depth = 0;
        for (int i = 0; i <= condition.length() - op.length(); i++) {
            char c = condition.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0
                    && condition.substring(i)
                    .toUpperCase()
                    .startsWith(op.toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    private String[] splitOnOperator(String condition, String op) {
        int depth = 0;
        for (int i = 0; i <= condition.length() - op.length(); i++) {
            char c = condition.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0
                    && condition.substring(i)
                    .toUpperCase()
                    .startsWith(op.toUpperCase())) {
                return new String[]{
                        condition.substring(0, i).trim(),
                        condition.substring(i + op.length()).trim()
                };
            }
        }
        return new String[]{condition, "true"};
    }
}