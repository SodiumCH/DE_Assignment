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
            return False
        if "metadata" not in raw_data or "payload" not in raw_data:
            return False
            
        metadata = raw_data["metadata"]
        payload = raw_data["payload"]
        
        if "event_id" not in metadata or "event_type" not in metadata:
            return False
            
        if "batch_id" not in payload or "action" not in payload:
            return False
        return True

    def process_kitchen(self, event_id, payload):
        batch = payload.get("batch_id", "Unknown-Batch")
        action = payload.get("action", "Unknown-Action")
        station = payload.get("station_id", "Unknown-Station")
        recipe = payload.get("recipe_id", "Unknown-Recipe")
        weight = payload.get("weight_kg", "Unknown-Weight")
        ingredients = payload.get("ingredients", "Unknown-Ingredients")
        temp = payload.get("temperature_celsius", "Unknown-Temperature")
        time = payload.get("event_timestamp", "Unknown-timestamp")
        
        details = f"Station: {station} | {recipe} | {weight}kg | {ingredients} | {temp}°C | {time}"
        print(f"[{self.kitchen_topic}] {batch} -> {action} | Meta ID: {event_id} | {details}")

    def process_dispatch(self, event_id, payload):
        batch = payload.get("batch_id", "Unknown-Batch")
        action = payload.get("action", "Unknown-Action")
        canteen = payload.get("canteen_id", "Unknown-Canteen")
        driver = payload.get("driver_id", "Unknown-Driver")
        temp = payload.get("truck_temp_celsius", "Unknown-Temperature")
        time = payload.get("event_timestamp", "Unknown-timestamp")
        
        details = f"Canteen: {canteen} | Driver: {driver} | {temp}°C | {time}"
        print(f"[{self.dispatch_topic}] {batch} -> {action} | Meta ID: {event_id} | {details}")

    def process_message(self, topic, key, raw_data):
        if not self.is_valid_envelope(raw_data):
            print(f"Warning: Bad message format received on {topic}. Sending to Dead Letter Queue.")
            if self.producer:
                self.producer.send(self.dlq_topic, key=key, value=raw_data)
            return

        metadata = raw_data["metadata"]
        payload = raw_data["payload"]
        event_id = metadata.get("event_id", "Unknown-ID")
        
        if topic == self.kitchen_topic:
            if self.producer:
                self.producer.send(self.clean_kitchen_topic, key=key, value=raw_data)
            self.process_kitchen(event_id, payload)
            
        elif topic == self.dispatch_topic:
            if self.producer:
                self.producer.send(self.clean_dispatch_topic, key=key, value=raw_data)
            self.process_dispatch(event_id, payload)

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