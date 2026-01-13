from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the Iceberg Catalog and Kafka packages
spark = (
    SparkSession.builder.appName("KafkaToIceberg")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-aws-bundle:1.4.3",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg_rest", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg_rest.type", "rest")
    .config("spark.sql.catalog.iceberg_rest.uri", "http://localhost:8181")
    .config(
        "spark.sql.catalog.rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config("spark.sql.catalog.iceberg_rest.s3.endpoint", "http://localhost:9000")
    .config("spark.sql.catalog.iceberg_rest.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg_rest.access-key-id", "admin")
    .config("spark.sql.catalog.iceberg_rest.secret-access-key", "password")
    .config(
        "spark.driver.extraJavaOptions",
        "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password",
    )
    .getOrCreate()
)


# Match the schema from your Python Producer
schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("item", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

# Read stream from Kafka
df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094,localhost:9095,localhost:9096")
    .option("subscribe", "user_purchases")
    .option("startingOffsets", "latest")
    .load()
)

# Convert binary value to string and parse JSON
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_rest.pyspark")

# Create the table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_rest.pyspark.purchases (
        user_id STRING,
        user_name STRING,
        item STRING,
        price DOUBLE,
        timestamp STRING,
        country STRING
    ) USING iceberg
    PARTITIONED BY (country)
""")


query = (
    df_parsed.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("path", "iceberg_rest.pyspark.purchases")
    .option("checkpointLocation", "checkpoints/iceberg_purchases")
    .start()
)

query.awaitTermination()
