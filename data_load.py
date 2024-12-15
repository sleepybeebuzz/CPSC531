# ETL Pipeline
# Transform data to model a snowflake schema for TPC-H benchmarking criteria
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (SparkSession.builder 
    .appName("TPC-H Snowflake Schema") 
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") 
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") 
    .config("spark.jars", "gs://tpch_1gb/archive/spark-bigquery-with-dependencies_2.12-0.41.0.jar") 
    .config("spark.bigquery.temp.gcs.bucket", "tpch_1gb")  # Only specify the bucket name
    .getOrCreate())

# Base Path to transfer processed data
gcs_base_path = "gs://tpch_1gb/archive/processed_data/"

schema_path = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

# Dictionary to store DataFrames
dfs = {}

# Loop over schema paths and read Parquet files into DataFrames
for path in schema_path:
    dfs[path] = spark.read.parquet(f"{gcs_base_path}{path}/")
    

for table, df in dfs.items():
    df.write.format('bigquery') \
    .option('table', f'cpsc531-project.tpch_1gb.{table}') \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("temporaryGcsBucket", "tpch_1gb") \
    .mode('overwrite') \
    .save()
