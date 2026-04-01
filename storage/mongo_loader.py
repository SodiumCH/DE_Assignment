import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pymongo import MongoClient
from utils.utility import load_config

class MongoLoader:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.mongo_uri = self.config.get("mongo_uri", "mongodb+srv://admin:admin@cluster0.nvfzbyz.mongodb.net/?appName=Cluster0")
        self.db_name = self.config.get("mongodb_db_name", "de_assignment")
        self.kitchen_collection = self.config.get("mongodb_kitchen_collection", "kitchen_events")
        self.dispatch_collection = self.config.get("mongodb_dispatch_collection", "dispatch_events")
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]        
        self.spark = self.setup_spark_session()

    def setup_spark_session(self):
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
        
        session = (SparkSession.builder
            .appName("CentralKitchenPipeline")
            .getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        return session

    def transfer_kitchen_data(self):
        print("Reading kitchen data with Spark...")
        kitchen_path = self.config.get("kitchen_data_path", "./output/kitchen_data")
        
        try:
            self.db[self.kitchen_collection].delete_many({})
            
            kitchen_df = self.spark.read.parquet(kitchen_path)
            
            print("Converting and sending to MongoDB Atlas...")
            records = [row.asDict(recursive=True) for row in kitchen_df.collect()]
            
            if records:
                self.db[self.kitchen_collection].insert_many(records)
                print(f"Success! Inserted {len(records)} kitchen events.")
            else:
                print("No kitchen data found to insert.")
                
        except Exception as error:
            print(f"Failed to move kitchen data. Error: {error}")

    def transfer_dispatch_data(self):
        print("\nReading dispatch data with Spark...")
        dispatch_path = self.config.get("dispatch_data_path", "./output/dispatch_data")
        
        try:
            self.db[self.dispatch_collection].delete_many({})

            dispatch_df = self.spark.read.parquet(dispatch_path)
            
            print("Converting and sending to MongoDB Atlas...")
            records = [row.asDict(recursive=True) for row in dispatch_df.collect()]
            
            if records:
                self.db[self.dispatch_collection].insert_many(records)
                print(f"Success! Inserted {len(records)} dispatch events.")
            else:
                print("No dispatch data found to insert.")
                
        except Exception as error:
            print(f"Failed to move dispatch data. Error: {error}")

    def run_transfer(self):
        self.transfer_kitchen_data()
        self.transfer_dispatch_data()
        print("\nClosing connections...")
        self.spark.stop()
        self.client.close()