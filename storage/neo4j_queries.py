from neo4j import GraphDatabase
from utils.utility import load_config
from datetime import datetime

class Neo4jQueries:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.uri = self.config.get("neo4j_uri", "neo4j+s://7bb39fe0.databases.neo4j.io")
        self.user = self.config.get("neo4j_user", "7bb39fe0")
        self.password = self.config.get("neo4j_pass", "5wCN9zh1-kH5f4ZODGGvU4V0i-DyreUtxfBHkjjIjis")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def track_ingredient_recall(self, target_ingredient, target_date):
        print(f"\n--- TRACKING RECALL FOR {target_ingredient} ON {target_date} ---")
        query = """
        MATCH (i:Ingredient {name: $ingredient})<-[:REQUIRES]-(r:Recipe)<-[:USES_RECIPE]-(b:Batch)-[:DESTINED_FOR]->(c:Canteen)
        MATCH (s:Station)-[p:PROCESSED]->(b)
        WHERE p.time STARTS WITH $date
        RETURN DISTINCT i.name AS Target_Ingredient, r.id AS Recipe_Used, b.id AS Batch_ID, c.id AS Affected_Canteen
        ORDER BY c.id
        """
        
        with self.driver.session() as session:
            results = session.run(query, ingredient=target_ingredient, date=target_date).data()
            
        if not results:
            print(f"No records found for ingredient '{target_ingredient}' on {target_date}.")
            return
            
        print(f"Found {len(results)} affected batches.")
        for record in results:
            recipe = record.get('Recipe_Used')
            batch = record.get('Batch_ID')
            canteen = record.get('Affected_Canteen')
            print(f"- {batch} (Recipe: {recipe}) delivered to Canteen {canteen}")

    def map_contamination_risk(self, target_station, target_date):
        print(f"\n--- MAPPING RISK FOR STATION {target_station} ON {target_date} ---")
        query = """
        MATCH (s:Station {id: $station})-[p1:PROCESSED]->(b:Batch)
        WHERE p1.time STARTS WITH $date
        OPTIONAL MATCH (later_s:Station)-[p2:PROCESSED]->(b)
        WHERE p2.time > p1.time
        WITH s, b, collect(DISTINCT later_s.id) AS Later_Stations
        MATCH (drv:Driver)-[:DELIVERS]->(b)-[:DESTINED_FOR]->(c:Canteen)
        RETURN s.id AS Risk_Station, b.id AS Shared_Batch, Later_Stations, drv.id AS Exposed_Driver, c.id AS Affected_Canteen
        ORDER BY b.id
        """
        
        with self.driver.session() as session:
            results = session.run(query, station=target_station, date=target_date).data()
            
        if not results:
            print(f"No connections found for station '{target_station}' on {target_date}.")
            return
            
        print(f"\nFound {len(results)} exposed batches and their respective paths.")
        for record in results:
            batch = record.get('Shared_Batch')
            later_stations = record.get('Later_Stations')
            driver = record.get('Exposed_Driver')
            canteen = record.get('Affected_Canteen')
            
            if later_stations:
                stations_text = ", ".join(later_stations)
            else:
                stations_text = "None"
                
            print(f"- {batch} -> Later Stations: [{stations_text}] delivered by Driver {driver} to Canteen {canteen}")

    def run_menu(self):        
        while True:
            print("\n" + "="*30)
            print("       NEO4J QUERY MENU")
            print("="*30)
            print("1. Track Ingredient Recall")
            print("2. Map Station Contamination Risk")
            print("0. Exit")
            choice = input("\nEnter your choice: ").strip()
            
            if choice == '0':
                print("Exiting the query tool.")
                break
                
            if choice in ['1', '2']:
                while True:
                    user_date = input("Enter the date to search (Format: YYYY-MM-DD): ").strip()
                    try:
                        datetime.strptime(user_date, "%Y-%m-%d")
                        break
                    except ValueError:
                        print("Error: The date must be in YYYY-MM-DD format. Please try again.")

                if choice == '1':
                    ingredient_input = None
                    while not ingredient_input:
                        ingredient_input = input("\nEnter the ingredient name to track (e.g., chicken): ").strip()
                        if not ingredient_input:
                            print("Error: The input cannot be empty.")
                    self.track_ingredient_recall(ingredient_input, user_date)
                    input("\nPress Enter to return to the menu...")
                    
                elif choice == '2':
                    station_input = None
                    while not station_input:
                        station_input = input("\nEnter the Station ID to map (e.g., cook_01): ").strip()
                        if not station_input:
                            print("Error: The input cannot be empty.")
                    self.map_contamination_risk(station_input, user_date)
                    input("\nPress Enter to return to the menu...")
                    
            else:
                print("\nInvalid choice. Please enter 1, 2, or 0.")

    def close(self):
        self.driver.close()