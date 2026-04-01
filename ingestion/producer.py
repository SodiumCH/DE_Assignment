import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from utils.utility import load_config
from ingestion.generator import EventGenerator

class EventProducer:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.producer = self.connectToKafka()

    def connectToKafka(self):
        servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print(f"Failed to connect to Kafka at {servers}. Check your network.")
            return None
            
    def on_send_error(self, excp):
        print(f"Failed to send message to Kafka: {excp}")

    def run(self):
        if not self.producer:
            print("Cannot start generator without a Kafka connection.")
            return

        gen = EventGenerator(self.config)
        print("Generator running... Press Ctrl+C to stop.")

        try:
            while True:
                status, events = gen.generate_event()

                if status == "DONE":
                    print("Limits reached. All batches delivered. Stopping generator.")
                    break

                if status == "MULTI":
                    for topic, data in events:
                        message_key = None
                        if type(data) is dict and "metadata" in data:
                            message_key = data["metadata"].get("event_id")
                        topic_name = self.config.get(topic, topic)
                        future = self.producer.send(topic_name, key=message_key, value=data)
                        future.add_errback(self.on_send_error)
                        
                #time.sleep(0.1)
        
        except KeyboardInterrupt:
            print("\nManual stop received.")
        except Exception as e:
            print(f"An unexpected error happened: {e}")
        finally:
            if self.producer:
                print("Closing Kafka connection...")
                self.producer.flush()
                self.producer.close()