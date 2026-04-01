import random
from datetime import datetime, timedelta
from ingestion.batch import Batch

class EventGenerator:
    def __init__(self, config_data):
        self.recipes = ["chicken_rice", "beef_stew", "veg_pasta", "fish_curry"]
        self.canteens = ["cant_01", "cant_02", "cant_03", "cant_04"]
        
        self.stations = {
            "prep_01": {"role": "PREPARING", "state": "IDLE", "batch_id": None},
            "prep_02": {"role": "PREPARING", "state": "IDLE", "batch_id": None},
            "prep_03": {"role": "PREPARING", "state": "IDLE", "batch_id": None},
            "prep_04": {"role": "PREPARING", "state": "IDLE", "batch_id": None},
            "cook_01": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "cook_02": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "cook_03": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "cook_04": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "cook_05": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "cook_06": {"role": "COOKING", "state": "IDLE", "batch_id": None},
            "pack_01": {"role": "PACKING", "state": "IDLE", "batch_id": None},
            "pack_02": {"role": "PACKING", "state": "IDLE", "batch_id": None},
            "pack_03": {"role": "PACKING", "state": "IDLE", "batch_id": None},
            "pack_04": {"role": "PACKING", "state": "IDLE", "batch_id": None}        
        }
        
        self.drivers_status = {
            "drv_01": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_02": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_03": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_04": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_05": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_06": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None},
            "drv_07": {"state": "IDLE", "canteen_id": None, "active_batches": 0, "return_time": None}
        }
        
        self.recipe_ingredients = {
            "chicken_rice": {"chicken": 0.5, "rice": 0.4, "water": 0.1},
            "beef_stew": {"beef": 0.4, "potato": 0.4, "onion": 0.1, "beef_broth": 0.1},
            "veg_pasta": {"pasta": 0.3, "tomato_sauce": 0.5, "water": 0.2},
            "fish_curry": {"white_fish": 0.5, "curry_paste": 0.3, "coconut_milk": 0.2}
        }
        
        self.active_batches = {} 
        self.counter = 1000
        self.kitchen_event_counter = 1000
        self.dispatch_event_counter = 1000
        
        self.start_hour = config_data.get("daily_start_hour", 6)
        self.stop_hour = config_data.get("daily_stop_hour", 21)
        self.sim_days = config_data.get("simulation_days", 30)
        
        self.start_date = datetime(2026, 3, 15, self.start_hour, 0, 0)
        self.global_clock = self.start_date
        self.end_of_simulation = self.start_date + timedelta(days=self.sim_days - 1, hours=(self.stop_hour - self.start_hour))
        
        self.stop_new_batches_today = False
        self.total_rows_made = 0

        self.source = config_data.get("data_source_name", "data_generator")

    def get_ingredients(self, recipe, total_weight):
        items = self.recipe_ingredients.get(recipe, {})
        if total_weight is None:
            return [{"item": name, "amount_kg": None} for name in items.keys()]
        return [{"item": name, "amount_kg": round(total_weight * ratio, 2)} 
                for name, ratio in items.items()]

    def update_clock(self):
        self.global_clock += timedelta(minutes=1)
        for d_id, status in self.drivers_status.items():
            if status["state"] == "RETURNING" and status["return_time"] <= self.global_clock:
                status["state"] = "IDLE"
                status["return_time"] = None
                status["canteen_id"] = None

    def check_day_limits(self):
        if self.global_clock.hour >= self.stop_hour:
            self.stop_new_batches_today = True

        all_drivers_idle = all(status["state"] == "IDLE" for status in self.drivers_status.values())
        
        if self.stop_new_batches_today and not self.active_batches and all_drivers_idle:
            if self.global_clock >= self.end_of_simulation:
                return "DONE"
            
            next_day = self.global_clock + timedelta(days=1)
            self.global_clock = next_day.replace(hour=self.start_hour, minute=0, second=0)
            self.stop_new_batches_today = False
        return "CONTINUE"

    def spawn_new_batches(self):
        if not self.stop_new_batches_today and ((random.random() < 0.25 and len(self.active_batches) < 8) or not self.active_batches):
            self.counter += 1
            b_id = f"BATCH-{self.counter}"
            self.active_batches[b_id] = Batch(b_id, random.choice(self.recipes), random.choice(self.canteens), self.global_clock)

    def get_target_state(self, current_state):
        next_states = {
            "START": "PREPARING", 
            "PREPARING": "COOKING", 
            "COOKING": "PACKING", 
            "PACKING": "LOADING",
            "LOADING": "DELIVERY",
            "DELIVERY": "DELIVERED"
        }
        return next_states.get(current_state)

    def assign_resources(self, batch, target_state):
        assigned_station = None
        assigned_driver = None
        
        if target_state in ["PREPARING", "COOKING", "PACKING"]:
            for s_id, s_info in self.stations.items():
                if s_info["role"] == target_state and s_info["state"] == "IDLE":
                    assigned_station = s_id
                    break
                    
        elif target_state == "LOADING":
            for d_id, status in self.drivers_status.items():
                if status["state"] == "LOADING" and status["canteen_id"] == batch.canteen_id:
                    assigned_driver = d_id
                    break
            if not assigned_driver:
                for d_id, status in self.drivers_status.items():
                    if status["state"] == "IDLE":
                        assigned_driver = d_id
                        break
                        
        return assigned_station, assigned_driver

    def update_resource_states(self, batch, target_state, assigned_station, assigned_driver):
        if batch.current_station_id and target_state in ["COOKING", "PACKING", "LOADING"]:
            self.stations[batch.current_station_id]["state"] = "IDLE"
            self.stations[batch.current_station_id]["batch_id"] = None
            
        if assigned_station:
            self.stations[assigned_station]["state"] = "BUSY"
            self.stations[assigned_station]["batch_id"] = batch.batch_id
            batch.current_station_id = assigned_station
            
        elif assigned_driver:
            batch.driver_id = assigned_driver
            self.drivers_status[assigned_driver]["state"] = "LOADING"
            self.drivers_status[assigned_driver]["canteen_id"] = batch.canteen_id
            self.drivers_status[assigned_driver]["active_batches"] += 1
            batch.current_station_id = None

    def build_payload(self, batch, b_id):
        current_state = batch.state
        ts = self.global_clock.isoformat(timespec='minutes')
        
        if current_state == "DELIVERY":
            self.drivers_status[batch.driver_id]["state"] = "DELIVERING"
        
        if current_state in ["PREPARING", "COOKING", "PACKING"]:
            topic = "kitchen_station_events"
            event_type = "kitchen_action"
            self.kitchen_event_counter += 1
            evt_id = f"KIT-EVT-{self.kitchen_event_counter}"
            
            if random.random() < 0.03:
                batch.current_weight = round(batch.current_weight - random.uniform(2, 5), 2)
            
            weight = batch.current_weight
            if random.random() < 0.05:
                weight = None

            if current_state == "PREPARING":
                temp = round(random.uniform(15.0, 22.0), 1) 
            elif current_state == "PACKING":
                temp = round(random.uniform(40.0, 60.0), 1) 
            else: 
                if random.random() < 0.10:
                    temp = round(random.uniform(50.0, 74.0), 1) 
                else:
                    temp = round(random.uniform(75.0, 100.0), 1) 
                
            if random.random() < 0.05:
                temp = None

            payload_data = {
                "batch_id": b_id,
                "station_id": batch.current_station_id,
                "recipe_id": batch.recipe,
                "action": current_state,
                "weight_kg": weight,
                "ingredients": self.get_ingredients(batch.recipe, batch.initial_weight),
                "temperature_celsius": temp,
                "event_timestamp": ts
            }
        else:
            topic = "dispatch_events"
            event_type = "dispatch_action"
            self.dispatch_event_counter += 1
            evt_id = f"DIS-EVT-{self.dispatch_event_counter}"
            
            if batch.truck_temp is not None:
                truck_temp = round(batch.truck_temp + random.uniform(-0.5, 0.5), 1)
            else:
                truck_temp = 4.0 
                
            if random.random() < 0.05:
                truck_temp = None
                
            payload_data = {
                "batch_id": b_id,
                "canteen_id": batch.canteen_id,
                "driver_id": batch.driver_id,
                "action": current_state,
                "truck_temp_celsius": truck_temp,
                "event_timestamp": ts
            }
            
        return topic, evt_id, event_type, payload_data

    def build_envelope(self, evt_id, event_type, payload_data, source):
        envelope = (
            {
                "metadata": {
                    "event_id": evt_id,
                    "event_type": event_type,
                    "source": source,
                    "generated_at": datetime.now().isoformat()
                },
                "payload": payload_data
            }
        )

        if random.random() < 0.05:
            fail_type = random.choice(["not_dict", "missing_meta", "missing_payload", "missing_id", "missing_type", "missing_action"])
            if fail_type == "not_dict":
                return "CORRUPT_RAW_STRING_DATA"
            elif fail_type == "missing_meta":
                del envelope["metadata"]
            elif fail_type == "missing_payload":
                del envelope["payload"]
            elif fail_type == "missing_id":
                if random.random() < 0.5:
                    del envelope["metadata"]["event_id"]
                else:
                    del envelope["payload"]["batch_id"]
            elif fail_type == "missing_type":
                del envelope["metadata"]["event_type"]
            elif fail_type == "missing_action":
                del envelope["payload"]["action"]

        return envelope
        
    def handle_delivery(self, batch, b_id):
        d_id = batch.driver_id
        self.drivers_status[d_id]["active_batches"] -= 1
        
        if self.drivers_status[d_id]["active_batches"] == 0:
            self.drivers_status[d_id]["state"] = "RETURNING"
            self.drivers_status[d_id]["return_time"] = self.global_clock + timedelta(minutes=batch.trip_duration)
            
        del self.active_batches[b_id]

    def generate_event(self):
        self.update_clock()
        
        if self.check_day_limits() == "DONE":
            return "DONE", None
            
        self.spawn_new_batches()
        
        events_to_send = []
        batch_ids = sorted(self.active_batches.keys())
        
        for b_id in batch_ids:
            batch = self.active_batches[b_id]
            
            if not batch.can_advance(self.global_clock):
                continue
                
            target_state = self.get_target_state(batch.state)
            assigned_station, assigned_driver = self.assign_resources(batch, target_state)
            
            if target_state in ["PREPARING", "COOKING", "PACKING"] and not assigned_station:
                continue
            if target_state == "LOADING" and not assigned_driver:
                continue
                
            self.update_resource_states(batch, target_state, assigned_station, assigned_driver)
            
            mins_for_next_task = batch.advance()
            batch.ready_time = self.global_clock + timedelta(minutes=mins_for_next_task)
            
            topic, evt_id, event_type, payload_data = self.build_payload(batch, b_id)
            source = self.source
            envelope = self.build_envelope(evt_id, event_type, payload_data, source)
            
            if batch.state == "DELIVERED":
                self.handle_delivery(batch, b_id)
                
            self.total_rows_made += 1
            events_to_send.append((topic, envelope))

        if events_to_send:
            return "MULTI", events_to_send
        return None, None