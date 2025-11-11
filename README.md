# material-master-delta-pipeline
# Material Master Data Delta Pipeline

**Owner:** chonkar.a@northeastern.edu  
**Assignment:** Delta Pipeline Implementation using Databricks Community Edition  
**Date:** November 2025

## Project Overview

This project implements a complete Delta Lake pipeline for processing material master data from multiple factories using Databricks Community Edition. The pipeline follows the medallion architecture (Bronze → Silver) with comprehensive data quality management.

## Architecture
```
Raw Data (Pipe-delimited CSV) → Bronze Layer → Silver Layer
```

## Implementation Details

### Bronze Layer (`01_Bronze_Layer_Material_Master.py`)
- **Purpose:** Raw data ingestion and storage
- **Input:** DBC 10 Material Master.csv (pipe-delimited, 1,000 records)
- **Processing:** Schema enforcement, metadata addition
- **Output:** Bronze temporary view with complete lineage tracking

### Silver Layer (`02_Silver_Layer_Material_Master.py`)
- **Purpose:** Data cleaning, standardization, and quality assessment
- **Data Cleaning:** Fixed 'abc' unit costs, standardized status values
- **Type Conversion:** String to numeric/date conversions
- **Data Expectation:** Material ID validation with success rate tracking
- **Quality Filtering:** Records with quality score >= 0.5
- **Output:** Silver temporary view ready for analytics

## Key Features

- ✅ Pipe-delimited CSV processing
- ✅ Delta Lake ACID compliance (temporary views due to Community Edition limitations)
- ✅ Comprehensive data quality scoring
- ✅ Data expectation implementation with monitoring
- ✅ Business rule validation and filtering
- ✅ Complete data lineage tracking

## Data Quality Results

- **Total Records Processed:** 1,000
- **Data Quality Issues Fixed:** Unit cost 'abc' values converted to null
- **Data Expectation Success Rate:** 100% (Material ID validation)
- **Records in Silver Layer:** ~950+ (after quality filtering)

## Environment

- **Platform:** Databricks Community Edition
- **Runtime:** Databricks Runtime with Spark 4.0.0
- **Language:** Python/PySpark
- **Storage:** Temporary views (Community Edition limitation workaround)

## Assignment Evidence

- **User Account:** chonkar.a@northeastern.edu
- **Workspace:** dbc-0eb243ec-4b35.cloud.databricks.com
- **Source File:** DBC 10 Material Master.csv (131,555 bytes)
- **Implementation:** Unity Catalog with temporary view storage

## Future Enhancements

- Gold layer implementation for aggregated analytics
- Automated scheduling with Databricks Jobs
- Advanced data quality monitoring
- Integration with external data sources

## Repository Structure
```
material-master-delta-pipeline/
├── notebooks/
│   ├── 01_Bronze_Layer_Material_Master.py
│   └── 02_Silver_Layer_Material_Master.py
├── screenshots/
│   ├── bronze_layer_execution.png
│   ├── silver_layer_results.png
│   └── data_quality_report.png
├── docs/
│   └── implementation_guide.md
├── data/
│   └── sample_data_structure.md
└── README.md
```

## How to Run

1. Import notebooks into Databricks workspace
2. Attach to running cluster
3. Execute Bronze layer first
4. Execute Silver layer second
5. Review data quality results

## Contact

For questions about this implementation, contact: chonkar.a@northeastern.edu
