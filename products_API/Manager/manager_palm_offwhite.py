import os
import csv
import pandas as pd
import uvicorn
from bs4 import BeautifulSoup
import json
import random
import time
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import gzip
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv
from fastapi import BackgroundTasks
from fastapi import FastAPI
from sqlalchemy import create_engine, text
app=FastAPI()
load_dotenv()
current_date=datetime.now().strftime("%m_%d_%Y")
current_directory= os.getcwd()
main_directory=os.path.join(current_directory,'outputs')

pwd_value = str(os.environ.get('MSSQLS_PWD'))
pwd_str =f"Pwd={pwd_value};"
global conn
conn = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
global engine
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)
def update_job_status(job_id):
    sql = (f"Update utb_BrandScanJobs\n"
           f"Set ParsingStart = getdate()\n"
           f"Where ID = {job_id}")
    if len(sql) > 0:
        ip = requests.get('https://api.ipify.org').content.decode('utf8')
        print('My public IP address is: {}'.format(ip))

        connection = engine.connect()
        sql = text(sql)
        print(sql)
        connection.execute(sql)
        connection.commit()
        connection.close()

def fetch_job_details(job_id):
    update_job_status(job_id)
    sql_query = (f"Select ParsingResultUrl,BrandID from utb_BrandScanJobs where ID = {job_id}")
    print(sql_query)
    df = pd.read_sql_query(sql_query, con=engine)
    print(df)
    engine.dispose()
    return df
def parse_brand_jobid(job_id):
    df=fetch_job_details(job_id)
    parsing_csv=str(df.iloc[0, 0])
    brand_id=int(df.iloc[0, 1])
    response=submit_job_post(job_id,brand_id,parsing_csv)
def submit_job_post(job_id,brand_id,parsing_csv):

    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }

    params = {
        'job_id' : job_id,
        'brand_id' : brand_id,
        'parsing_csv': f"{parsing_csv}"
    }

    response = requests.post(f"{os.environ.get('AGENT_BASE_URL')}/run_parser", params=params, headers=headers)
    return response.status_code
def update_sql_job(job_id, resultUrl, count):
    sql = (
            f"Update utb_BrandScanJobs Set ParsingResultUrl = '{resultUrl}',\n"
            f"ParsingCount =  {count},\n"
            f" ParsingEnd = getdate()\n"
            f" Where ID = {job_id}"
           )
    if len(sql) > 0:
        ip = requests.get('https://api.ipify.org').content.decode('utf8')
        print('My public IP address is: {}'.format(ip))

        connection = engine.connect()
        sql = text(sql)
        print(sql)
        connection.execute(sql)
        connection.commit()
        connection.close()
@app.post("/submit_job")
async def brand_single(job_id: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(parse_brand_jobid, job_id)

    return {"message": "Notification sent in the background"}
@app.post("/job_complete")
async def brand_batch_endpoint(job_id: str, resultUrl:str,count:int,background_tasks: BackgroundTasks):
    background_tasks.add_task(update_sql_job, job_id, resultUrl, count)

    return {"message": "Notification sent in the background"}

if __name__ == "__main__":
    uvicorn.run("manager_palm_offwhite:app", port=8008, log_level="info")