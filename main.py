import sys
import time
import multiprocessing
from reset import SystemCleanup
from ingestion.producer import EventProducer
from ingestion.consumer import EventConsumer
from processing.spark_stream_processor import SparkStructuredStreaming
from processing.batch_processor import BatchProcessor
from storage.mongo_loader import MongoLoader
from storage.neo4j_loader import Neo4jLoader
from storage.mongo_queries import MongoQueries
from storage.neo4j_queries import Neo4jQueries

def run_streamer():
    streamer = SparkStructuredStreaming()
    streamer.start_streaming()

def run_consumer():
    consumer = EventConsumer()
    consumer.run()

def run_producer():
    publisher = EventProducer()
    publisher.run()

class MainMenu:
    def __init__(self):
        self.running = True

    def display_menu(self):
        print("\n" + "="*40)
        print("    CENTRAL KITCHEN DATA PIPELINE")
        print("="*40)
        print("1. Run Full Pipeline (Produce Data & Load DBs)")
        print("2. Run Batch Processing (Spark Reports)")
        print("3. Run MongoDB Queries")
        print("4. Run Neo4j Queries")
        print("5. Reset System (Clear Kafka, DBs, HDFS)")
        print("0. Exit")
        print("="*40)

    def run_full_pipeline(self):
        print("\nStarting the automated data pipeline.")
        
        stream_process = multiprocessing.Process(target=run_streamer)
        consumer_process = multiprocessing.Process(target=run_consumer)
        producer_process = multiprocessing.Process(target=run_producer)

        print("Starting Spark Stream Processor...")
        stream_process.start()
        time.sleep(15) 

        print("Starting Kafka Consumer...")
        consumer_process.start()
        time.sleep(5) 

        print("Starting Kafka Producer...")
        producer_process.start()
        
        producer_process.join()
        print("Producer finished generating data.")

        print("Waiting for final events to process...")
        time.sleep(15)
        
        print("Stopping Stream Processor and Consumer...")
        stream_process.terminate()
        consumer_process.terminate()
        stream_process.join()
        consumer_process.join()

        print("Loading data into MongoDB...")
        mongo_loader = MongoLoader()
        mongo_loader.run_transfer()

        print("Loading data into Neo4j...")
        neo4j_loader = Neo4jLoader()
        neo4j_loader.run_transfer()

        print("Full pipeline execution complete.")

    def run(self):
        while self.running:
            self.display_menu()
            choice = input("Enter your choice (0-5): ").strip()

            if choice == '1':
                self.run_full_pipeline()
            elif choice == '2':
                processor = BatchProcessor()
                processor.run_tests()
            elif choice == '3':
                queries = MongoQueries()
                queries.run_menu()
                queries.close()
            elif choice == '4':
                queries = Neo4jQueries()
                queries.run_menu()
                queries.close()
            elif choice == '5':
                cleaner = SystemCleanup()
                cleaner.run()
            elif choice == '0':
                print("Exiting application.")
                self.running = False
            else:
                print("Invalid choice. Please try again.")

if __name__ == "__main__":
    app = MainMenu()
    app.run()