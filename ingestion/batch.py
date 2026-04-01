import random

class Batch:
    def __init__(self, batch_id, recipe, canteen_id, start_time):
        self.batch_id = batch_id
        self.recipe = recipe
        self.canteen_id = canteen_id
        self.driver_id = None
        self.state = "START" 
        self.ready_time = start_time 
        self.current_station_id = None 
        self.initial_weight = round(random.uniform(10.0, 50.0), 2)
        self.current_weight = self.initial_weight
        self.truck_temp = None 
        self.trip_duration = 0 
        self.simulated_durations = (
            {
                "START": 0, 
                "PREPARING": random.randint(15, 20), 
                "COOKING": random.randint(25, 55), 
                "PACKING": random.randint(10, 25), 
                "LOADING": random.randint(10, 15), 
                "DELIVERY": random.randint(30, 60)
            }
        )

    def can_advance(self, current_sim_time):
        return current_sim_time >= self.ready_time

    def advance(self):
        transitions = (
            {
                "START": "PREPARING", 
                "PREPARING": "COOKING", 
                "COOKING": "PACKING", 
                "PACKING": "LOADING", 
                "LOADING": "DELIVERY", 
                "DELIVERY": "DELIVERED"
            }
        )
        
        new_state = transitions.get(self.state, "DELIVERED")
        mins_to_add = self.simulated_durations.get(new_state, 0)
        
        if self.state == "COOKING" and new_state == "PACKING":
            self.current_weight = round(
                self.current_weight * random.uniform(0.97, 0.99), 2
            )
        
        if new_state == "LOADING" and self.truck_temp is None:
            if random.random() < 0.10:
                self.truck_temp = round(random.uniform(6.0, 15.0), 1)
            else:
                self.truck_temp = round(random.uniform(-18.0, 5.0), 1)
                
        if new_state == "DELIVERY":
            self.trip_duration = mins_to_add
                
        self.state = new_state
        return mins_to_add