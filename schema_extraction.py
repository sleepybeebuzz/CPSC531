from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

# Base Path to transfer processed data
gcs_base_path = "gs://tpch_1gb/archive/processed_data/"


# Initialize Spark Session
spark = (SparkSession.builder 
    .appName("TPC-H Snowflake Schema") 
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") 
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") 
    .config("spark.jars", "gs://tpch_1gb/archive/spark-3.5-bigquery-0.41.0.jar") 
    .getOrCreate())
    
# Set up the necessary BigQuery connector
# spark.conf.set("spark.jars", "gs://tpch_1gb/archive/spark-3.5-bigquery-0.41.0.jar")


# Load Part Table
part_schema = StructType([
    StructField("P_PARTKEY", IntegerType(), True),
    StructField("P_NAME", StringType(), True),
    StructField("P_MFGR", StringType(), True),
    StructField("P_BRAND", StringType(), True),
    StructField("P_TYPE", StringType(), True),
    StructField("P_SIZE", IntegerType(), True),
    StructField("P_CONTAINER", StringType(), True),
    StructField("P_RETAILPRICE", DecimalType(), True),
    StructField("P_COMMENT", StringType(), True)
])
part_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(part_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/part.tbl")
part_df.write.mode("overwrite").parquet(f"{gcs_base_path}part/")

# Load Supplier Table
supplier_schema = StructType([
    StructField("S_SUPPKEY", IntegerType(), True),
    StructField("S_NAME", StringType(), True),
    StructField("S_ADDRESS", StringType(), True),
    StructField("S_NATIONKEY", IntegerType(), True),
    StructField("S_PHONE", StringType(), True),
    StructField("S_ACCTBAL", DecimalType(), True),
    StructField("S_COMMENT", StringType(), True)
])
supplier_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(supplier_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/supplier.tbl")
supplier_df.write.mode("overwrite").parquet(f"{gcs_base_path}supplier/")

# Load Parts Supplier Table
partsupp_schema = StructType([
    StructField("PS_PARTKEY", IntegerType(), True),
    StructField("PS_SUPPKEY", IntegerType(), True),
    StructField("PS_AVAILQTY", IntegerType(), True),
    StructField("PS_SUPPLYCOST", DecimalType(), True),
    StructField("PS_COMMENT", StringType(), True)
])
partsupp_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(partsupp_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/partsupp.tbl")
partsupp_df.write.mode("overwrite").parquet(f"{gcs_base_path}partsupp/")

# Load Customer Table
customer_schema = StructType([
    StructField("C_CUSTKEY", IntegerType(), True),
    StructField("C_NAME", StringType(), True),
    StructField("C_ADDRESS", StringType(), True),
    StructField("C_NATIONKEY", IntegerType(), True),
    StructField("C_PHONE", StringType(), True),
    StructField("C_ACCTBAL", DecimalType(), True),
    StructField("C_MKTSEGMENT", StringType(), True),
    StructField("C_COMMENT", StringType(), True)
])
customer_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(customer_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/customer.tbl")
customer_df.write.mode("overwrite").parquet(f"{gcs_base_path}customer/")

# Load Orders Table
order_schema = StructType([
    StructField("O_ORDERKEY", IntegerType(), True),
    StructField("O_CUSTKEY", IntegerType(), True),
    StructField("O_ORDERSTATUS", StringType(), True),
    StructField("O_TOTALPRICE", DecimalType(), True),
    StructField("O_ORDERDATE", DateType(), True),
    StructField("O_ORDERPRIORITY", StringType(), True),
    StructField("O_CLERK", StringType(), True),
    StructField("O_SHIPPRIORITY", IntegerType(), True),
    StructField("O_COMMENT", StringType(), True)
])
order_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(order_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/orders.tbl")
order_df.write.mode("overwrite").parquet(f"{gcs_base_path}orders/")    

# Load LineItem Table
lineitem_schema = StructType([
    StructField("L_ORDERKEY", IntegerType(), False),          # Foreign Key to O_ORDERKEY
    StructField("L_PARTKEY", IntegerType(), False),           # Foreign Key to P_PARTKEY
    StructField("L_SUPPKEY", IntegerType(), False),           # Foreign Key to S_SUPPKEY
    StructField("L_LINENUMBER", IntegerType(), False),        # Line item number (integer)
    StructField("L_QUANTITY", DecimalType(10, 2), False),     # Quantity as decimal
    StructField("L_EXTENDEDPRICE", DecimalType(10, 2), False), # Extended price as decimal
    StructField("L_DISCOUNT", DecimalType(5, 2), False),      # Discount as decimal
    StructField("L_TAX", DecimalType(5, 2), False),           # Tax as decimal
    StructField("L_RETURNFLAG", StringType(), False),         # Return flag (fixed text, size 1)
    StructField("L_LINESTATUS", StringType(), False),         # Line status (fixed text, size 1)
    StructField("L_SHIPDATE", DateType(), False),             # Ship date
    StructField("L_COMMITDATE", DateType(), False),           # Commit date
    StructField("L_RECEIPTDATE", DateType(), False),          # Receipt date
    StructField("L_SHIPINSTRUCT", StringType(), False),       # Shipping instructions (fixed text, size 25)
    StructField("L_SHIPMODE", StringType(), False),           # Shipping mode (fixed text, size 10)
    StructField("L_COMMENT", StringType(), False)             # Comment (variable text size 44)
])
lineitem_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(lineitem_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/lineitem.tbl")
lineitem_df.write.mode("overwrite").parquet(f"{gcs_base_path}lineitem/")    
# # Saving to BigQuery
# lineitem_df.write.format("bigquery") \
#     .option("table", "your_project_id.your_dataset_id.lineitem") \
#     .option("temporaryGcsBucket", "your_temp_gcs_bucket") \
#     .mode("overwrite") \
#     .save()

# Example of Saving DataFrame in Parquet format
# lineitem_df.write.mode("overwrite").parquet("gs://your-bucket-name/output/lineitem/")    

# Load Nation Table
nation_schema = StructType([
    StructField("N_NATIONKEY", IntegerType(), False),         # Primary key for nation (identifier)
    StructField("N_NAME", StringType(), False),               # Nation name (fixed text, size 25)
    StructField("N_REGIONKEY", IntegerType(), False),         # Foreign Key to R_REGIONKEY
    StructField("N_COMMENT", StringType(), False)             # Comment (variable text size 152)
])
nation_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(nation_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/nation.tbl")
nation_df.write.mode("overwrite").parquet(f"{gcs_base_path}nation/")    

# Load Region Table
region_schema = StructType([
    StructField("R_REGIONKEY", IntegerType(), False),         # Primary key for region (identifier)
    StructField("R_NAME", StringType(), False),               # Region name (fixed text, size 25)
    StructField("R_COMMENT", StringType(), False)             # Comment (variable text size 152)
])
region_df = spark.read.format("csv") \
    .option("delimiter", "|") \
    .schema(region_schema) \
    .load("gs://tpch_1gb/archive/tpc-h/region.tbl")
region_df.write.mode("overwrite").parquet(f"{gcs_base_path}region/")


