import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient
from neo4j import GraphDatabase
from utils.utility import load_config

class SystemCleanup:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        
        self.bootstrap_servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        self.topics = [
            self.config.get("kitchen_station_events", "kitchen_station_events"),
            self.config.get("dispatch_events", "dispatch_events"),
            self.config.get("clean_kitchen_events", "clean_kitchen_events"),
            self.config.get("clean_dispatch_events", "clean_dispatch_events"),
            self.config.get("dlq_topic", "dead_letter_queue")
        ]
        
        self.mongo_uri = self.config.get("mongo_uri", "mongodb+srv://admin:admin@cluster0.nvfzbyz.mongodb.net/?appName=Cluster0")
        self.db_name = self.config.get("mongodb_db_name", "de_assignment")
        self.kitchen_coll = self.config.get("mongodb_kitchen_collection", "kitchen_events")
        self.dispatch_coll = self.config.get("mongodb_dispatch_collection", "dispatch_events")
        
        self.neo_uri = self.config.get("neo4j_uri", "neo4j+s://7bb39fe0.databases.neo4j.io")
        self.neo_user = self.config.get("neo4j_user", "7bb39fe0")
        self.neo_pass = self.config.get("neo4j_pass", "5wCN9zh1-kH5f4ZODGGvU4V0i-DyreUtxfBHkjjIjis")
        
        self.hdfs_path = "./output"

    def clean_kafka(self):
        print(f"Deleting Kafka topics: {self.topics}")
        admin_client = None
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            admin_client.delete_topics(topics=self.topics)
            time.sleep(5) 
            
            new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in self.topics]
            admin_client.create_topics(new_topics=new_topics)
            print("Kafka topics are ready.")
        except Exception as e:
            print(f"Kafka error: {e}")
        finally:
            if admin_client:
                admin_client.close()

    def clean_mongodb(self):
        print(f"Cleaning MongoDB: Dropping {self.kitchen_coll} and {self.dispatch_coll}...")
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            db[self.kitchen_coll].drop()
            db[self.dispatch_coll].drop()
            print("MongoDB collections are empty.")
            client.close()
        except Exception as e:
            print(f"MongoDB error: {e}")

    def clean_neo4j(self):
        print("Cleaning Neo4j: Deleting nodes and relationships...")
        try:
            driver = GraphDatabase.driver(self.neo_uri, auth=(self.neo_user, self.neo_pass))
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
            print("Neo4j graph is empty.")
            driver.close()
        except Exception as e:
            print(f"Neo4j error: {e}")

    def clean_hdfs(self):
        print(f"Cleaning HDFS path: {self.hdfs_path}")
        cmd = f"hdfs dfs -rm -r -skipTrash {self.hdfs_path}/*"
        response = os.system(cmd)
        
        if response == 0:
            print("HDFS path is clean.")
        else:
            print("HDFS path is already empty.")

    def run(self):
        print("--- Starting Full Project Cleanup ---")
        self.clean_kafka()
        #self.clean_mongodb()
        self.clean_neo4j()
        self.clean_hdfs()
        print("--- Cleanup Finished ---")