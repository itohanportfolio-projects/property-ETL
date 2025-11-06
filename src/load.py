import sqlalchemy
import psycopg2
from dotenv import load_dotenv
import  os                                              
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import time
import random
from concurrent.futures import ThreadPoolExecutor

           

load_dotenv()
#read load files from transofm folder into db

file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "transform")
   
files= [
        os.path.join(file_path, f)
        for f in os.listdir(file_path)
        if os.path.isfile(os.path.join(file_path,f))
    ]

def load_file(file_path):
      
        split_path = file_path.split("\\transform\\")
        property_location = split_path[-1].replace("properties_data_", "").replace(".csv", "")
      
        db_host = os.getenv('host')
        db_name = os.getenv('database_name')
        db_pass = os.getenv('password')
        db_port = os.getenv('port')
        db_user = os.getenv('username')

        print(db_name)
        # engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

        engine = create_engine('postgresql+psycopg2://postgres:password!12@localhost:5432/prime_estate')
        #for file in file_path.iterdir():
        load_file_path=os.path.join(file_path,file)  
        df = pd.read_csv (load_file_path)
        df.to_sql(property_location, engine, if_exists='replace', index=False)
        #test connection string 
        # with engine.connect() as conn:
        #     print("Connected successfully!")

                


# for file in files:
#    if not file.endswith(".csv"):
#         print(f"{file} extension is not valid") 
#         continue
#    else:
#         load_data=load_file(file)
with ThreadPoolExecutor(max_workers=len(files)) as executor:
       futures =executor.map(load_file,files)