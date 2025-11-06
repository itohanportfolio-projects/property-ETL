import json
import pandas as pd
#import shutil
import os
from pathlib import Path
import time
import random
from concurrent.futures import ThreadPoolExecutor


#get files in folder and store in a list
file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
trans_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "transform")

os.makedirs(file_path, exist_ok=True)
os.makedirs(trans_path, exist_ok=True)

files= [
        os.path.join(file_path, f)
        for f in os.listdir(file_path)
        if os.path.isfile(os.path.join(file_path,f))
    ]



def transform(file_path):
        
        #split the path by raw and pet the name of properties_
        split_path = file_path.split("\\raw\\")
        property_location = split_path[-1].replace("properties_data_", "").replace(".json", "")

        with open(file_path,"r") as f:
            data = json.load(f)
        #normalize tha date to flatten the json into a table
        df = pd.json_normalize(data)
        #select the columns required
        colunms =[
            'id', 
            'formattedAddress', 
            'city', 
            'state', 
            'stateFips',
            'zipCode',
            'county', 
            'countyFips', 
            'latitude', 
            'longitude', 
            'propertyType', 
            'bedrooms', 
            'bathrooms', 
            'squareFootage', 
            'yearBuilt'
        ]
        df = df [colunms]

        #standardize the column names

        df.rename(columns={'formattedAddress':'address',
                        'stateFips':'state_fips',
                        'countyFips' :'county_fips',
                        'propertyType' : 'property_type', 
                        'squareFootage': 'square_footage' ,
                        'yearBuilt' : 'year_built'
                        
                        },inplace=True)
       
        #write transformed data to local 
        trans_path_name =  os.path.join(trans_path, f"properties_data_{property_location}.csv")
        df.to_csv(trans_path_name,index=False)



# for file in files:
#    if not file.endswith(".json"):
#         print(f"{file} extension is not valid") 
#         continue
#    else:
#         trans_data=transform(file)

with ThreadPoolExecutor(max_workers=len(files)) as executor:
       futures =executor.map(transform,files)