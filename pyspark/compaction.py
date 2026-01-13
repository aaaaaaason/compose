from pyspark.sql import SparkSession

# Define the Iceberg Catalog and Kafka packages
spark = (
    SparkSession.builder.appName("Compaction")
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


spark.sql("""
    CALL iceberg_rest.system.rewrite_data_files(
        table => 'pyspark.purchases',
        options => map('target-file-size-bytes', '536870912')
    )
""").show()
