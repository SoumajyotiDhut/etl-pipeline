# Multi-Tenant ETL Pipeline System

A production-ready ETL (Extract, Transform, Load) pipeline system built with
Java Spring Boot. Supports multiple data sources, configurable transformations,
and multiple output destinations with full execution tracking.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Java 17 | Programming language |
| Spring Boot 3.2.5 | Application framework |
| Spring Batch | Pipeline execution engine |
| PostgreSQL | Primary database |
| Flyway | Database migrations |
| Lombok | Reduces boilerplate code |
| Swagger / OpenAPI | API documentation |
| JUnit 5 | Testing framework |
| Maven | Build tool |

---

## Prerequisites

Before running this project, make sure you have installed:

- Java JDK 17 or higher
- Maven 3.9+
- PostgreSQL 14+
- Git

---

## Setup Instructions

### Step 1: Clone the Repository
```bash
git clone https://github.com/SoumajyotiDhut/etl-pipeline.git
cd etl-pipeline/etl-pipeline
```

### Step 2: Create the Database

Open pgAdmin or psql and run:
```sql
CREATE DATABASE etl_pipeline_db;
```

### Step 3: Configure Database Connection

Open `src/main/resources/application.properties` and update:
```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/etl_pipeline_db
spring.datasource.username=postgres
spring.datasource.password=YOUR_PASSWORD_HERE
```

### Step 4: Build the Project
```bash
mvn clean install -DskipTests
```

### Step 5: Run the Application
```bash
mvn spring-boot:run
```

The application starts on `http://localhost:8080`

### Step 6: Verify Setup

Open your browser and go to:
```
http://localhost:8080/swagger-ui/index.html
```

You should see all API endpoints listed.

---

## Running Tests
```bash
mvn test
```

Expected output:
```
Tests run: 24, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

---

## API Endpoints

### Pipeline Management

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/pipelines` | Create a new pipeline |
| GET | `/api/v1/pipelines` | Get all pipelines |
| GET | `/api/v1/pipelines/{id}` | Get pipeline by ID |
| PUT | `/api/v1/pipelines/{id}` | Update a pipeline |
| DELETE | `/api/v1/pipelines/{id}` | Delete a pipeline |

### Pipeline Execution

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/pipelines/{id}/run` | Trigger pipeline execution |
| GET | `/api/v1/runs/{jobRunId}` | Get job run status |
| GET | `/api/v1/pipelines/{id}/runs` | Get all runs for a pipeline |
| GET | `/api/v1/runs?tenantId=x` | Get all runs for a tenant |
| DELETE | `/api/v1/runs/{id}/cancel` | Cancel a running job |

### Testing Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/ingest/csv` | Test CSV ingestion |
| POST | `/api/v1/ingest/json` | Test JSON ingestion |
| POST | `/api/v1/transform/test` | Test transformation chain |
| POST | `/api/v1/destination/test` | Test full ETL flow |

---

## Quick Start: Run Your First Pipeline

### Step 1: Create a Pipeline
```bash
POST http://localhost:8080/api/v1/pipelines
Content-Type: application/json
```

Use the body from `samples/sample_csv_pipeline.json`

### Step 2: Run the Pipeline
```bash
POST http://localhost:8080/api/v1/pipelines/sales_etl_001/run
```

### Step 3: Check Status
```bash
GET http://localhost:8080/api/v1/runs/{jobRunId}
```

Expected response when complete:
```json
{
  "data": {
    "status": "SUCCESS",
    "recordsRead": 7,
    "recordsWritten": 3,
    "recordsFiltered": 2,
    "durationSeconds": 1
  }
}
```

---

## Project Structure
```
etl-pipeline/
├── src/main/java/com/etlpipeline/
│   ├── controller/          REST API controllers
│   ├── service/             Business logic
│   ├── repository/          Database access layer
│   ├── model/               JPA entities and domain models
│   ├── dto/                 Request and response objects
│   ├── ingestion/           CSV and JSON file readers
│   ├── transformation/      Filter, Map, Aggregate engines
│   ├── destination/         Database and file writers
│   ├── execution/           Pipeline orchestration
│   ├── exception/           Error handling
│   └── config/              Spring configuration
├── src/main/resources/
│   ├── application.properties
│   ├── db/migration/        Flyway SQL scripts
│   └── sample-data/         Sample CSV and JSON files
├── src/test/                JUnit test classes
├── samples/                 Sample pipeline JSON definitions
└── README.md
```

---

## Supported Features

### Data Sources
- CSV files (configurable delimiter, encoding, headers)
- JSON files (array, object, and JSON Lines formats)

### Transformations
- **Filter** — remove rows using conditions
- **Map** — rename columns, derive new columns, apply functions
- **Aggregate** — group and compute SUM, AVG, COUNT, MIN, MAX

### Destinations
- **Database** — PostgreSQL with append, overwrite, upsert modes
- **File** — CSV or JSON output with optional gzip compression

### Execution
- Async pipeline execution
- Status tracking: PENDING → RUNNING → SUCCESS / FAILED / CANCELLED
- Full metrics: records read, written, filtered, failed, duration
- Multi-tenant isolation by tenant_id

---

## Sample Data

Sample files are in `src/main/resources/sample-data/`:
- `sales.csv` — 7 sales transactions
- `products.json` — 5 product catalog entries

Sample pipeline definitions are in the `samples/` folder.

---

## API Documentation

Swagger UI:
```
http://localhost:8080/swagger-ui/index.html
```

Postman collection:
```
samples/ETL_Pipeline_API.postman_collection.json
```

---

## 👨‍💻 Built By

**Soumajyoti Dhut**  
Internship Technical Assessment — Evolving Systems Ltd  