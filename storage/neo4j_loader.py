import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from utils.utility import load_config

class Neo4jLoader:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.uri = self.config.get("neo4j_uri", "neo4j+s://7bb39fe0.databases.neo4j.io")
        self.user = self.config.get("neo4j_user", "7bb39fe0")
        self.password = self.config.get("neo4j_pass", "5wCN9zh1-kH5f4ZODGGvU4V0i-DyreUtxfBHkjjIjis")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.spark = self.setup_spark_session()

    def setup_spark_session(self):
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
        
        session = (SparkSession.builder
            .appName("CentralKitchenPipeline")
            .getOrCreate())
        session.sparkContext.setLogLevel("WARN")
        return session

    def transfer_kitchen_data(self):
        print("Reading kitchen data with Spark...")
        kitchen_path = self.config.get("kitchen_data_path", "./output/kitchen_data")
        
        try:
            kitchen_df = self.spark.read.parquet(kitchen_path)
            records = [row.asDict(recursive=True) for row in kitchen_df.collect()]
            
            if records:
                print("Sending kitchen events to Neo4j Aura...")
                query = """
                UNWIND $events AS event
                MERGE (b:Batch {id: toString(event.batch_id)})
                MERGE (s:Station {id: toString(event.station_id)})
                MERGE (r:Recipe {id: toString(event.recipe_id)})
                MERGE (s)-[rel:PROCESSED]->(b)
                SET rel.action = event.action,
                    rel.time = event.event_timestamp,
                    rel.temp_celsius = event.temperature_celsius,
                    rel.weight = event.weight_kg
                MERGE (b)-[:USES_RECIPE]->(r)
                
                WITH event, r
                WHERE event.ingredients IS NOT NULL
                UNWIND event.ingredients AS ing
                MERGE (i:Ingredient {name: toString(ing.item)})
                MERGE (r)-[req:REQUIRES]->(i)
                """
                with self.driver.session() as session:
                    session.run(query, events=records)
                print(f"Success. Inserted {len(records)} kitchen events.")
            else:
                print("No kitchen data found.")
                
        except Exception as error:
            print(f"Failed to move kitchen data. Error: {error}")

    def transfer_dispatch_data(self):
        print("\nReading dispatch data with Spark...")
        dispatch_path = self.config.get("dispatch_data_path", "./output/dispatch_data")
        
        try:
            dispatch_df = self.spark.read.parquet(dispatch_path)
            records = [row.asDict(recursive=True) for row in dispatch_df.collect()]
            
            if records:
                print("Sending dispatch events to Neo4j Aura...")
                query = """
                UNWIND $events AS event
                MERGE (b:Batch {id: toString(event.batch_id)})
                MERGE (d:Driver {id: toString(event.driver_id)})
                MERGE (c:Canteen {id: toString(event.canteen_id)})
                MERGE (d)-[rel:DELIVERS]->(b)
                SET rel.action = event.action,
                    rel.time = event.event_timestamp,
                    rel.truck_temp_celsius = event.truck_temp_celsius
                MERGE (b)-[:DESTINED_FOR]->(c)
                """
                with self.driver.session() as session:
                    session.run(query, events=records)
                print(f"Success. Inserted {len(records)} dispatch events.")
            else:
                print("No dispatch data found.")
                
        except Exception as error:
            print(f"Failed to move dispatch data. Error: {error}")

    def run_transfer(self):
        self.transfer_kitchen_data()
        self.transfer_dispatch_data()
        print("\nClosing connections...")
        self.spark.stop()
        self.driver.close()