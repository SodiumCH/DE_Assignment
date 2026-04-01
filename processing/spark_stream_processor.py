import pyspark
from utils.utility import load_config
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import col, from_json, when, expr, round

class SparkStructuredStreaming:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.spark = self.setup_spark_session()
        self.define_schemas()

    def setup_spark_session(self):
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            
        spark_version = pyspark.__version__
        session = (
            SparkSession.builder
            .appName("CentralKitchenPipeline")
            .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("OFF")
        return session

    def define_schemas(self):
        ingredient_schema = StructType([
            StructField("item", StringType(), True),
            StructField("amount_kg", DoubleType(), True)
        ])

        metadata_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("generated_at", StringType(), True)
        ])

        kitchen_payload_schema = StructType([
            StructField("batch_id", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("recipe_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("weight_kg", DoubleType(), True),
            StructField("ingredients", ArrayType(ingredient_schema), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("event_timestamp", StringType(), True)
        ])

        dispatch_payload_schema = StructType([
            StructField("batch_id", StringType(), True),
            StructField("canteen_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("truck_temp_celsius", DoubleType(), True),
            StructField("event_timestamp", StringType(), True)
        ])

        self.kitchen_envelope = StructType([
            StructField("metadata", metadata_schema, True),
            StructField("payload", kitchen_payload_schema, True)
        ])

        self.dispatch_envelope = StructType([
            StructField("metadata", metadata_schema, True),
            StructField("payload", dispatch_payload_schema, True)
        ])

    def read_kafka_topic(self, servers, topic):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("subscribe", topic)
            .load()
        )

    def parse_envelope(self, df, schema):
        return (
            df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.metadata.*", "data.payload.*")
        )

    def process_kafka_stream(self, df):
        return (df.selectExpr(
            "CAST(key AS STRING) as message_key",
            "CAST(value AS STRING) as raw_message",
            "timestamp as event_timestamp"
        ))

    def apply_kitchen_business_rules(self, df):
        min_temp = self.config.get("min_cook_temp", 75.0)
        min_drop = self.config.get("min_weight_drop", 2.0)

        df_calc = df.withColumn(
            "expected_start_weight",
            round(expr("aggregate(ingredients, 0D, (acc, item) -> acc + item.amount_kg)"), 2)
        )
        
        return df_calc.withColumn(
            "sensor_status",
            when(col("weight_kg").isNull() | col("temperature_celsius").isNull(), "SENSOR_FAILURE").otherwise("OK")
        ).withColumn(
            "temperature_status",
            when((col("action") == "COOKING") & (col("temperature_celsius") < min_temp), "LOW_HEAT_WARNING").otherwise("OK")
        ).withColumn(
            "weight_status",
            when(col("weight_kg").isNull(), "UNKNOWN")
            .when((col("action").isin("PREPARING", "COOKING", "PACKING")) & 
                  ((col("expected_start_weight") - col("weight_kg")) >= min_drop), "SUSPICIOUS_DROP")
            .otherwise("OK")
        )

    def apply_dispatch_business_rules(self, df):
        max_temp = self.config.get("max_truck_temp", 5.0)
        return df.withColumn(
            "sensor_status",
            when(col("truck_temp_celsius").isNull(), "SENSOR_FAILURE").otherwise("OK")
        ).withColumn(
            "temperature_status",
            when(col("truck_temp_celsius") > max_temp, "WARM_TRUCK_WARNING").otherwise("OK")
        )

    def cleanup_resources(self, kitchen_q=None, dispatch_q=None, dlq_q=None, raw_kitchen_q=None, raw_dispatch_q=None):
        print("Starting resource cleanup...")
        
        for query in [kitchen_q, dispatch_q, dlq_q, raw_kitchen_q, raw_dispatch_q]:
            if query and query.isActive:
                print(f"Stopping stream query: {query.id}")
                query.stop()
        
        if self.spark:
            print("Closing Spark Session...")
            self.spark.stop()
        
        print("Cleanup complete.")

    def start_streaming(self):
        servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        
        kitchen_topic = self.config.get("clean_kitchen_events", "clean_kitchen_events")
        kitchen_raw = self.read_kafka_topic(servers, kitchen_topic)
        
        raw_kitchen = self.process_kafka_stream(kitchen_raw)
        raw_kitchen_query = (
            raw_kitchen.writeStream.format("parquet")
            .option("path", self.config.get("raw_kitchen_data_path", "./output/raw_kitchen_data"))
            .option("checkpointLocation", self.config.get("raw_kitchen_checkpoint_path", "./output/checkpoints/raw_kitchen"))
            .start()
        )

        transformed_kitchen = self.apply_kitchen_business_rules(self.parse_envelope(kitchen_raw, self.kitchen_envelope))
        kitchen_query = (
            transformed_kitchen.writeStream.format("parquet") 
            .option("path", self.config.get("kitchen_data_path", "./output/kitchen_data")) 
            .option("checkpointLocation", self.config.get("kitchen_checkpoint_path", "./output/checkpoints/kitchen_checkpoint_path")).start()
        )

        dispatch_topic = self.config.get("clean_dispatch_events", "clean_dispatch_events")
        dispatch_raw = self.read_kafka_topic(servers, dispatch_topic)
        
        raw_dispatch = self.process_kafka_stream(dispatch_raw)
        raw_dispatch_query = (
            raw_dispatch.writeStream.format("parquet")
            .option("path", self.config.get("raw_dispatch_data_path", "./output/raw_dispatch_data"))
            .option("checkpointLocation", self.config.get("raw_dispatch_checkpoint_path", "./output/checkpoints/raw_dispatch"))
            .start()
        )

        transformed_dispatch = self.apply_dispatch_business_rules(self.parse_envelope(dispatch_raw, self.dispatch_envelope))
        dispatch_query = (
            transformed_dispatch.writeStream.format("parquet")
            .option("path", self.config.get("dispatch_data_path", "./output/dispatch_data"))
            .option("checkpointLocation", self.config.get("dispatch_checkpoint_path", "./output/checkpoints/dispatch_checkpoint_path")).start()
        )

        dlq_topic = self.config.get("dlq_topic", "dead_letter_queue")
        dlq_raw = self.read_kafka_topic(servers, dlq_topic)
        dlq_transformed = self.process_kafka_stream(dlq_raw)
        dlq_query = (
            dlq_transformed.writeStream.format("parquet")
            .option("path", self.config.get("dlq_data_path", "./output/dlq_data"))
            .option("checkpointLocation", self.config.get("dlq_checkpoint_path", "./output/checkpoints/dlq"))
            .start())

        print(f"Streaming is active. Monitoring for data in {kitchen_topic}, {dispatch_topic}, and {dlq_topic}.")

        try:
            self.spark.streams.awaitAnyTermination()
        except Exception as error:
            print(f"A stream error occurred: {error}")
        finally:
            self.cleanup_resources(kitchen_query, dispatch_query, dlq_query, raw_kitchen_query, raw_dispatch_query)