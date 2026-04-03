import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyspark
from utils.utility import load_config
from pyspark.sql import SparkSession

class DeadLetterMonitor:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.spark = self.setup_spark_session()

    def setup_spark_session(self):
        spark_version = pyspark.__version__
        session = (
            SparkSession.builder
            .appName("DLQViewer")
            .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}")
            .getOrCreate()
        )
        return session

    def display_failures(self):
        servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        dlq_topic = self.config.get("dlq_topic", "dead_letter_queue")

        print(f"Fetching messages from: {dlq_topic}")

        df = (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("subscribe", dlq_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        logs = df.selectExpr(
            "CAST(key AS STRING) as message_key", 
            "CAST(value AS STRING) as error_payload", 
            "timestamp"
        )
        
        logs.show(truncate=False)