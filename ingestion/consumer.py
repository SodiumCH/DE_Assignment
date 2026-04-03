import json
from utils.utility import load_config 
from kafka import KafkaConsumer, KafkaProducer

class EventConsumer:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.kitchen_topic = self.config.get("kitchen_station_events", "kitchen_station_events")
        self.dispatch_topic = self.config.get("dispatch_events", "dispatch_events")
        self.dlq_topic = self.config.get("dlq_topic", "dead_letter_queue")
        self.clean_kitchen_topic = self.config.get("clean_kitchen_events", "clean_kitchen_events")
        self.clean_dispatch_topic = self.config.get("clean_dispatch_events", "clean_dispatch_events")        
        self.servers = self.config.get("kafka_bootstrap_servers", "localhost:9092")
        self.consumer = None
        self.producer = None

    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                self.kitchen_topic,
                self.dispatch_topic,
                bootstrap_servers=self.servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='event_consumer_group',
                enable_auto_commit=True
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=self.servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            print("Connected to Kafka. Listening for events...")
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka server: {e}")
            return False

    def is_valid_envelope(self, raw_data):
        if type(raw_data) is not dict:
            return ("false", "Unknown-Batch")
            
        if "payload" not in raw_data:
            return ("false", "Unknown-Batch")

        payload = raw_data["payload"]

        if "metadata" not in raw_data:
            return ("false", payload.get("batch_id", "Unknown-Batch"))
            
        metadata = raw_data["metadata"]
        
        if "event_id" not in metadata or "event_type" not in metadata:
            return ("false", payload.get("batch_id", "Unknown-Batch"))
            
        if "batch_id" not in payload or "action" not in payload:
            return ("false", payload.get("batch_id", "Unknown-Batch"))
        return ("true", payload.get("batch-id", "Unknown-Batch"))

    def process_message(self, topic, key, raw_data):
        valid_envelope = self.is_valid_envelope(raw_data)
        if valid_envelope[0] == "false":
            if self.producer:
                self.producer.send(self.dlq_topic, key=key, value=raw_data)
                print(f"Bad message format detected for {valid_envelope[1]}. Publishing to [{self.dlq_topic}].")
                return
                
        payload = raw_data["payload"]
        batch = payload.get("batch_id", "Unknown-Batch")
        action = payload.get("action", "Unknown-Action")
        
        if topic == self.kitchen_topic:
            if self.producer:
                self.producer.send(self.clean_kitchen_topic, key=key, value=raw_data)
                print(f"Published {batch} ({action}) to [{self.clean_kitchen_topic}]")
            
        elif topic == self.dispatch_topic:
            if self.producer:
                self.producer.send(self.clean_dispatch_topic, key=key, value=raw_data)
                print(f"Published {batch} ({action}) to [{self.clean_dispatch_topic}]")

    def run(self):
        if not self.connect():
            return

        print("(Press Ctrl+C to stop)")
        try:
            for message in self.consumer:
                self.process_message(message.topic, message.key, message.value)
        except KeyboardInterrupt:
            print("\nManual stop received. Closing connections.")
        except Exception as e:
            print(f"\nAn unexpected error stopped the consumer: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
                self.producer.close()
            print("Kafka connections closed safely.")