import requests
import json
from src.config import BASE_URL, API_KEY


#define a dictionary for city and state
locations = [
    {"city": "San Antonio", "state": "TX"},
    {"city": "Los Angeles", "state": "CA"},
    {"city": "Phoenix", "state": "AZ"}
]

def extract_properties(city,state):
        
              
            headers ={ 
                "accept": "application/json", 
                "X-Api-Key":  API_KEY
                }  
            
            params = {
                "city": city,
                "state": state
            }      
            
                         
            print(f"fetching properties data for {city}_{state}")  
            response =requests.get(BASE_URL,headers = headers ,params= params) 
            if response.status_code == 200:
                data = response.json()    
                    #return data 
                file_name = f"data/raw/properties_data_{city}_{state}.json"
                with open(file_name, "w" ) as f:
                    json.dump(data, f , indent=4)
                    return file_name
                    
            else:
                    print({response.status_code} - {response.text})
                    return None    
            
#loop through locations and extract data for each       
for loc in locations:
    data = extract_properties(loc["city"],loc["state"])
