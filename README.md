# NorthStar Retail – Cloud Data Engineering Pipeline

## Project Overview

The **NorthStar Retail Cloud Data Engineering Pipeline** is an Azure-based data engineering project designed to process and analyze large volumes of retail sales data.

The project demonstrates the **Separation of Storage and Compute** paradigm by using Azure Data Lake Storage Gen2 for data storage, Azure Data Factory for workflow orchestration, and Azure Databricks with PySpark for data transformation.

The pipeline automates daily data processing and generates regional sales summaries for business analysis.

## Architecture & Technologies

| Component | Technology | Purpose |
|---|---|---|
| Storage | Azure Data Lake Storage Gen2 | Stores raw and processed sales data |
| Orchestration | Azure Data Factory | Automates and schedules pipeline execution |
| Compute | Azure Databricks | Performs data transformations using PySpark |
| Analytics | Azure Synapse Analytics | Validates and queries processed data |
| Data Format | Parquet, CSV | Stores structured sales data |

## Architecture Workflow

1. **Data Ingestion:** Raw sales data in Parquet format is uploaded to the `raw-data` container in Azure Data Lake Storage Gen2.

2. **Pipeline Orchestration:** Azure Data Factory manages the workflow through a pipeline named `SalesPipeline`.

3. **Scheduling:** A scheduled trigger is configured to execute the pipeline daily at midnight.

4. **Data Transformation:** Azure Databricks processes the raw sales data using PySpark.
   - Reads Parquet files from the storage layer.
   - Filters transactions where `total_sales > 100`.
   - Groups transactions by region.
   - Calculates total sales and transaction counts.
   - Sorts the results by total sales in descending order.

5. **Data Storage:** The transformed results are saved in the `processed-data` container in Parquet format.

6. **Data Validation:** Azure Synapse Serverless SQL is used to query and validate the sales aggregation results.

## Data Transformation Logic

The following PySpark operations are performed:

```python
from pyspark.sql.functions import col, sum, count, desc

df_processed = (
    df_raw
    .filter(col("total_sales") > 100)
    .groupBy("region")
    .agg(
        sum("total_sales").alias("total_sales"),
        count("transaction_id").alias("transaction_count")
    )
    .orderBy(desc("total_sales"))
)
```

## SQL Validation

The aggregated sales data can be validated using Azure Synapse Serverless SQL.

```sql
SELECT
    region,
    ROUND(SUM(total_sales), 2) AS total_sales,
    COUNT(transaction_id) AS transaction_count
FROM
    OPENROWSET(
        BULK 'https://adlspoornimasales.dfs.core.windows.net/raw-data/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE
    total_sales > 100
GROUP BY
    region
ORDER BY
    total_sales DESC;
```

## Repository Structure

```text
northstar-retail-sales-pipeline/
│
├── scripts/
│   ├── spark_transformation_pipeline.py
│   └── synapse_sales_aggregation.sql
│
├── data/
│   ├── raw_sales_data.csv
│   └── aggregated_sales_summary.csv
│
└── README.md
```

## Azure Configuration

**Storage Account:** `adlspoornimasales`

**Containers:**
- `raw-data` – Stores original sales data.
- `processed-data` – Stores transformed sales results.

**Azure Data Factory Pipeline:** `SalesPipeline`

**Trigger Schedule:** Daily at 12:00 AM (Midnight).

## Key Features

- Automated daily data pipeline execution.
- Scalable cloud-based data storage.
- Separation of storage and compute resources.
- Large-scale data transformation using PySpark.
- Regional sales aggregation and analysis.
- SQL-based data validation.
- Parquet-based data processing for efficient analytics.

## Expected Outcome

The pipeline produces a regional sales summary containing:

- Region
- Total Sales
- Transaction Count

Only transactions with sales values greater than $100 are included in the aggregation.

## Learning Outcomes

Through this project, I gained practical experience in:

- Azure Data Lake Storage Gen2.
- Azure Data Factory pipeline creation and scheduling.
- Azure Databricks and PySpark transformations.
- Azure Synapse Serverless SQL.
- Cloud data engineering concepts.
- Separation of Storage and Compute architecture.

## Author

**Poornima Arunachalam**

B.Tech – Information Technology

GitHub: [PoornimaArunachalam](https://github.com/PoornimaArunachalam)
