from pymongo import MongoClient
from utils.utility import load_config

class MongoQueries:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.mongo_uri = self.config.get("mongo_uri", "mongodb+srv://admin:admin@cluster0.nvfzbyz.mongodb.net/?appName=Cluster0")
        self.db_name = self.config.get("mongodb_db_name", "de_assignment")
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.kitchen = self.db[self.config.get("mongodb_kitchen_collection", "kitchen_events")]
        self.dispatch = self.db[self.config.get("mongodb_dispatch_collection", "dispatch_events")]

    def search_batch_journey(self, target_batch_id):
        print(f"\n--- SEARCHING JOURNEY FOR BATCH: {target_batch_id} ---")
        
        kitchen_records = list(self.kitchen.find({"batch_id": target_batch_id}))
        dispatch_records = list(self.dispatch.find({"batch_id": target_batch_id}))
        
        print(f"Found {len(kitchen_records)} kitchen events and {len(dispatch_records)} dispatch events.")
        
        for record in kitchen_records:
            record["source"] = "Kitchen"
            
        for record in dispatch_records:
            record["source"] = "Dispatch"
            
        all_records = kitchen_records + dispatch_records
        all_records.sort(key=lambda x: x.get("event_timestamp", ""))
        
        print("\nUnified Batch Timeline:")
        for record in all_records:
            timestamp = record.get("event_timestamp")
            action = record.get("action")
            source = record.get("source")
            
            if source == "Kitchen":
                location = f"Station {record.get('station_id')}"
                print(f"- {timestamp} | [{source}] {action} at {location}")
            else:
                driver = f"Driver {record.get('driver_id')}"
                print(f"- {timestamp} | [{source}] {action} by {driver}")

    def find_fastest_drivers(self):
        print("\n--- FASTEST DELIVERY DRIVERS ---")
        
        pipeline = [
            {"$match": {"action": {"$in": ["LOADING", "DELIVERED"]}}},
            {"$group": {
                "_id": {"batch_id": "$batch_id", "driver_id": "$driver_id"},
                "start_time": {"$min": "$event_timestamp"},
                "end_time": {"$max": "$event_timestamp"}
            }},
            {"$addFields": {
                "delivery_time_mins": {
                    "$dateDiff": {
                        "startDate": {"$toDate": "$start_time"},
                        "endDate": {"$toDate": "$end_time"},
                        "unit": "minute"
                    }
                }
            }},
            {"$match": {"delivery_time_mins": {"$gt": 0}}},
            {"$group": {
                "_id": "$_id.driver_id",
                "min_time": {"$min": "$delivery_time_mins"},
                "max_time": {"$max": "$delivery_time_mins"},
                "avg_time": {"$avg": "$delivery_time_mins"},
                "total_deliveries": {"$sum": 1}
            }},
            {"$sort": {"avg_time": 1}}
        ]
        
        results = list(self.dispatch.aggregate(pipeline))
        
        if not results:
            print("No completed deliveries found to calculate times.")
            return
            
        print("Driver Leaderboard by Average Minutes Used per Delivery:")
        for rank, driver in enumerate(results, 1):
            driver_id = driver.get('_id')
            min_time = round(driver.get('min_time', 0), 1)
            max_time = round(driver.get('max_time', 0), 1)
            avg_time = round(driver.get('avg_time', 0), 1)
            deliveries = driver.get('total_deliveries', 0)
            print(f"{rank}. Driver {driver_id}: Min/Max/Average: {min_time}/{max_time}/{avg_time} minutes ({deliveries} deliveries)")

    def run_menu(self):
        while True:
            print("\n" + "="*30)
            print("         QUERY MENU")
            print("="*30)
            print("1. Search Batch Journey")
            print("2. Find Fastest Delivery Drivers")
            print("0. Exit")
            
            choice = input("\nEnter your choice: ").strip()
            
            if choice == '0':
                print("Exiting the query tool.")
                break
                
            if choice == '1':
                batch_input = input("Enter the Batch ID to search: ").strip()
                
                if not batch_input:
                    print("Error: The Batch ID cannot be empty.")
                    continue
                    
                kitchen_count = self.kitchen.count_documents({"batch_id": batch_input})
                dispatch_count = self.dispatch.count_documents({"batch_id": batch_input})
                
                if kitchen_count == 0 and dispatch_count == 0:
                    print(f"Error: No records found for batch '{batch_input}'. Please check the ID and try again.")
                    continue
                    
                self.search_batch_journey(batch_input)
                input("\nPress Enter to return to the menu...")
                
            elif choice == '2':
                self.find_fastest_drivers()
                input("\nPress Enter to return to the menu...")
                
            else:
                print("Invalid choice. Please enter 1, 2, or 0.")

    def close(self):
        self.client.close()