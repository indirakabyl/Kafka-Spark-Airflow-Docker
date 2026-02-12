import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")
logger = logging.getLogger("spark_structured_streaming")

BROKERS = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
TOPIC = "names_topic"

# Match YOUR producer payload
SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("nation", StringType(), True),
    StructField("zip", StringType(), True),            # safer as string (md5 int can be huge)
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("email", StringType(), True),
])

def initialize_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized")
    return spark

def get_streaming_dataframe(spark: SparkSession):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    logger.info("Streaming dataframe created")
    return df

def transform_streaming_data(df):
    transformed = (
        df.selectExpr("CAST(value AS STRING) AS value_str")
          .withColumn("data", from_json(col("value_str"), SCHEMA))
          .select("data.*")
    )
    return transformed

def write_stream(df, output_path: str, checkpoint_path: str):
    logger.info(f"Writing stream to: {output_path}")
    q = (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
    q.awaitTermination()

def main():
    spark = initialize_spark_session("SparkStructuredStreamingNames")

    raw = get_streaming_dataframe(spark)
    parsed = transform_streaming_data(raw)

    # ✅ Start local first (inside spark container filesystem)
    output_path = "/tmp/names_parquet"
    checkpoint = "/tmp/checkpoints/names_topic"

    write_stream(parsed, output_path, checkpoint)

if __name__ == "__main__":
    main()
