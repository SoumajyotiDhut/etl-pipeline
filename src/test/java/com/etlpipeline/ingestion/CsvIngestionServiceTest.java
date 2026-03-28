package com.etlpipeline.ingestion;

import com.etlpipeline.model.IngestionResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test 4: CSV Ingestion
 * Creates real temporary CSV files and tests ingestion.
 * @TempDir creates a temp folder that is deleted after each test.
 */
class CsvIngestionServiceTest {

    private CsvIngestionService     csvIngestionService;
    private SchemaInferenceService  schemaInferenceService;

    // JUnit creates and cleans up this temp directory automatically
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        schemaInferenceService = new SchemaInferenceService();
        csvIngestionService    = new CsvIngestionService(
                schemaInferenceService);
    }

    // Helper to create a real CSV file in the temp directory
    private String createCsvFile(String filename,
                                 String content) throws IOException {
        File file = tempDir.resolve(filename).toFile();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file.getAbsolutePath();
    }

    // ── TEST 4.1 ──────────────────────────────────────────────────
    @Test
    @DisplayName("CSV ingestion reads all rows correctly")
    void testCsvIngestionReadsAllRows() throws IOException {
        // ARRANGE — create a real CSV file
        String csvContent =
                "id,name,amount\n" +
                        "1,Alice,100.50\n" +
                        "2,Bob,200.75\n" +
                        "3,Charlie,50.00\n";

        String filePath = createCsvFile("test.csv", csvContent);

        Map<String, Object> config = new HashMap<>();
        config.put("file_path",  filePath);
        config.put("delimiter",  ",");
        config.put("has_header", true);
        config.put("encoding",   "utf-8");

        // ACT
        IngestionResult result = csvIngestionService.ingest(config);

        // ASSERT
        assertTrue(result.isSuccess(),
                "Ingestion should succeed");
        assertEquals(3, result.getRecords().size(),
                "Should read 3 data rows");
        assertEquals(3, result.getSuccessfulRows(),
                "All 3 rows should be successful");
        assertEquals(0, result.getFailedRows(),
                "No rows should fail");
    }

    // ── TEST 4.2 ──────────────────────────────────────────────────
    @Test
    @DisplayName("CSV ingestion detects correct column names")
    void testCsvIngestionDetectsColumnNames() throws IOException {
        // ARRANGE
        String csvContent =
                "transaction_id,product_id,quantity,price\n" +
                        "TXN001,PROD_A,5,100.00\n";

        String filePath = createCsvFile("cols.csv", csvContent);

        Map<String, Object> config = new HashMap<>();
        config.put("file_path",  filePath);
        config.put("has_header", true);

        // ACT
        IngestionResult result = csvIngestionService.ingest(config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertTrue(result.getColumnNames()
                        .contains("transaction_id"),
                "Should detect transaction_id column");
        assertTrue(result.getColumnNames()
                        .contains("product_id"),
                "Should detect product_id column");
        assertTrue(result.getColumnNames()
                        .contains("quantity"),
                "Should detect quantity column");
        assertTrue(result.getColumnNames()
                        .contains("price"),
                "Should detect price column");
    }

    // ── TEST 4.3 ──────────────────────────────────────────────────
    @Test
    @DisplayName("CSV ingestion returns failed result for missing file")
    void testCsvIngestionFailsForMissingFile() {
        // ARRANGE — file that does not exist
        Map<String, Object> config = new HashMap<>();
        config.put("file_path", "/nonexistent/path/file.csv");

        // ACT
        IngestionResult result = csvIngestionService.ingest(config);

        // ASSERT
        assertFalse(result.isSuccess(),
                "Ingestion should fail for missing file");
        assertNotNull(result.getGeneralError(),
                "Should have an error message");
        assertTrue(result.getGeneralError()
                        .contains("not found"),
                "Error should mention file not found");
    }

    // ── TEST 4.4 ──────────────────────────────────────────────────
    @Test
    @DisplayName("CSV ingestion infers integer type for numeric column")
    void testCsvIngestionInfersIntegerType() throws IOException {
        // ARRANGE
        String csvContent =
                "id,score\n" +
                        "1,95\n" +
                        "2,87\n" +
                        "3,92\n";

        String filePath = createCsvFile("types.csv", csvContent);

        Map<String, Object> config = new HashMap<>();
        config.put("file_path",  filePath);
        config.put("has_header", true);

        // ACT
        IngestionResult result = csvIngestionService.ingest(config);

        // ASSERT — id should be Long not String
        assertTrue(result.isSuccess());
        Object idValue = result.getRecords()
                .get(0).getFields().get("id");

        assertTrue(idValue instanceof Long,
                "id should be inferred as Long/Integer type, "
                        + "but was: " + idValue.getClass().getSimpleName());
    }

    // ── TEST 4.5 ──────────────────────────────────────────────────
    @Test
    @DisplayName("CSV ingestion handles pipe delimiter correctly")
    void testCsvIngestionHandlesPipeDelimiter() throws IOException {
        // ARRANGE — pipe separated file
        String csvContent =
                "name|city|country\n" +
                        "Alice|London|UK\n" +
                        "Bob|Paris|France\n";

        String filePath = createCsvFile("pipe.csv", csvContent);

        Map<String, Object> config = new HashMap<>();
        config.put("file_path",  filePath);
        config.put("delimiter",  "|");
        config.put("has_header", true);

        // ACT
        IngestionResult result = csvIngestionService.ingest(config);

        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRecords().size(),
                "Should read 2 records from pipe-delimited file");
        assertEquals("Alice",
                result.getRecords().get(0).getFields().get("name"),
                "First record name should be Alice");
    }
}
