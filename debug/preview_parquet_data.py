import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utility import load_config
from pyspark.sql import SparkSession

class ParquetDataViewer:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.kitchen_path = self.config.get("kitchen_data_path", "./output/kitchen_data")
        self.dispatch_path = self.config.get("dispatch_data_path", "./output/dispatch_data")
        self.raw_kitchen_path = self.config.get("raw_kitchen_data_path", "./output/raw_kitchen_data")
        self.raw_dispatch_path = self.config.get("raw_dispatch_data_path", "./output/raw_dispatch_data")
        self.spark = self.setup_spark_session()

    def setup_spark_session(self):
        session = SparkSession.builder \
            .appName("ReadSavedData") \
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")
        return session

    def preview_parquet_data(self, path, dataset_name):
        print(f"\n=== Reading {dataset_name} Data ===")
        try:
            df = self.spark.read.parquet(path)
            df.orderBy("event_timestamp").show(200, truncate=False)
        except Exception as e:
            print(f"No {dataset_name.lower()} data found at: {path}")

    def run(self):
        self.preview_parquet_data(self.kitchen_path, "Raw Kitchen")
        self.preview_parquet_data(self.kitchen_path, "Kitchen")
        self.preview_parquet_data(self.dispatch_path, "Raw Dispatch")
        self.preview_parquet_data(self.dispatch_path, "Dispatch")

    def close(self):
        if self.spark:
            self.spark.stop()