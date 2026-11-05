NorthStar Retail - Cloud Data Engineering PipelineAn enterprise-grade data ingestion and transformation pipeline built on Microsoft Azure, designed to demonstrate the Separation of Storage and Compute paradigm for high-volume retail sales analytics.
🏗️ Architecture & Tech StackThis project decouples storage and compute layers to ensure elastic scalability, cost-efficiency, and fault tolerance as transaction volumes increase:Storage Layer: Azure Data Lake Storage (ADLS) Gen2 (adlspoornimasales) for secure, scalable raw and processed Parquet data storage.Orchestration Layer: Azure Data Factory (ADF) to automate and schedule pipeline triggers daily at midnight.Compute & Transformation Layer: Azure Databricks (PySpark) and Azure Synapse Analytics to process large-scale datasets, filter transactions, and compute regional aggregations.
🔄 Workflow OverviewIngestion: Raw transaction Parquet files (.parquet) are landed in the raw container (raw-data/) of ADLS Gen2.   Orchestration: An Azure Data Factory pipeline (SalesPipeline) triggers daily at midnight to initiate the workflow.   Transformation (Compute):Reads raw sales parquet data into a Spark dataframe.Filters out transactions under $100 (total_sales > 100).Aggregates total sales and transaction counts grouped by sales region.Persistence: The processed analytical results are written back to the processed container (processed-data/) in ADLS Gen2.  Repository StructurePlaintextnorthstar-retail-sales-pipeline/
│
├── scripts/
│   ├── spark_transformation_pipeline.py  # PySpark script for Databricks transformation
│   └── synapse_sales_aggregation.sql     # Synapse SQL query for data validation
│
├── data/
│   ├── raw_sales_data.csv                # Sample raw sales dataset input
│   └── aggregated_sales_summary.csv      # Processed regional sales output
│
└── README.md                             # Project documentation
🚀 Getting Started & Execution1. Configure Storage PathsUpdate the storage account and container references in your Spark or ADF configurations:Pythonstorage_account_name = "adlspoornimasales"
container_name = "salesdata"
2. Run the Transformation ScriptExecute the PySpark script (spark_transformation_pipeline.py) inside your Azure Databricks cluster or Synapse Spark pool:Pythondf_processed = df_raw.filter(col("total_sales") > 100) \
    .groupBy("region") \
    .agg(
        sum("total_sales").alias("total_sales"),
        count("transaction_id").alias("transaction_count")
    ) \
    .orderBy(desc("total_sales"))
3. Validate Output via SQLYou can query the Parquet files directly from ADLS Gen2 using Azure Synapse Serverless SQL (synapse_sales_aggregation.sql):SQLSELECT 
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
Pipeline Outputs & VerificationADF Pipeline Monitor: Successfully running daily schedule with verified activity completions.   Raw Data Lake: Contains partitioned Parquet sales files (sales_transactions_2026_q1.parquet, etc.).   Processed Data Lake: Contains finalized columnar parquet summaries partitioned by region.   
