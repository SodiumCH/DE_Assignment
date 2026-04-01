import json

def load_config(filename='config.json'): 
    try:
        with open(filename, 'r') as config:
            return json.load(config)
            
    except FileNotFoundError:
        print(f"Error: {filename} not found.")
        return {}