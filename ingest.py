from feast.infra.contrib.stream_processor import ProcessorConfig
from feast.infra.contrib.spark_kafka_processor import SparkProcessorConfig
from feast.infra.contrib.stream_processor import get_stream_processor_object
import pandas as pd;
from feast import FeatureStore
from pyspark.sql import SparkSession

store = FeatureStore(repo_path="./")

def preprocess_fn(rows: pd.DataFrame):
    print(f"df columns: {rows.columns}")
    print(f"df size: {rows.size}")
    print(f"df preview:\n{rows.head()}")
    return rows

# Replace the version with your specific Spark version
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"

# Initialize a Spark session with Kafka support
spark = SparkSession.builder \
    .appName("Feast Streaming Ingestion") \
    .master("local[*]") \
    .config("spark.jars.packages", kafka_package) \
.getOrCreate()


ingestion_config = SparkProcessorConfig(mode="spark", source="kafka", spark_session=spark, processing_time="30 seconds", query_timeout=15)
sfv = store.get_stream_feature_view("driver_hourly_stats_stream")

processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=sfv,
    preprocess_fn=preprocess_fn,
)
processor.ingest_stream_feature_view();