import os
import csv
import uuid
from io import StringIO

import boto3
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

class FarfetchProductParser:
    def __init__(self, product_url_list):
        self.product_url_list = product_url_list
        self.serverless_urls=["https://router-proxy-google-cloud-2-o5empm7y3q-pd.a.run.app/fetch","https://router-proxy-google-cloud-o5empm7y3q-pd.a.run.app/fetch"]
    def get_product_html_list(self):
         return [self.get_product_html(product_url) for product_url in self.product_url_list]
    def get_product_html(self,product_url):
        return open_link(self.serverless_urls, product_url)
    def parse_product_details(self, soup):
        details_section = soup.find('div', {'data-testid': 'product-information-accordion'})
        if not details_section:
            details_section = soup.find('div', {'data-component': 'TabsContainer'})

        product_info = {'farfetch_id': '', 'brand_style_id': ''}

        if details_section:
            # Extract correct product id
            composition_elements = details_section.find_all('div', {'class': 'ltr-92qs1a'})
            for comp in composition_elements:
                heading = comp.find('h4', {'data-component': 'BodyBold'}).get_text(strip=True)
                if heading == 'Product IDs':
                    product_ids = comp.find_all('p', {'class': 'ltr-4y8w0i-Body'})
                    for pid in product_ids:
                        if 'FARFETCH ID:' in pid.get_text():
                            product_info['farfetch_id'] = pid.find('span').get_text(strip=True)
                        if 'Brand style ID:' in pid.get_text():
                            product_info['brand_style_id'] = pid.find('span').get_text(strip=True)
        print(f"This is the product info:\n{product_info}")
        return product_info

    def parse(self):
        all_product_details = []
        product_html_list=self.get_product_html_list()
        for product_html in product_html_list:
            soup = BeautifulSoup(product_html, 'html.parser')
            product_details = self.parse_product_details(soup)
            all_product_details.append(product_details)
        return all_product_details


@staticmethod
def open_link(serverless_urls,url_in):
    while True:
        payload = json.dumps({
            "url": url_in
        })
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3',
            'Content-Type': 'application/json'
        }
        url=random.choice(serverless_urls)

        response = requests.request("POST", url, headers=headers, data=payload)
        response = json.loads(response.text)
        print(f"Current serverless URL: {url}")
        print(f"Trying to open {url_in}")
        print(response.get('result')[:1000])
        time.sleep(3)
        if not ("Access Denied" in response.get('result',"Access Denied") or "429 Too Many Requests" in response.get('result',"429 Too Many Requests")):
            break

    return response.get('result')



serverless_urls=["https://router-proxy-google-cloud-2-o5empm7y3q-pd.a.run.app/fetch","https://router-proxy-google-cloud-o5empm7y3q-pd.a.run.app/fetch"]


def get_product_url_list(brand_product_df, farfetch_id_list):
    def extract_id(url):
        return url.split('-')[-1].replace('.aspx', '')

    brand_product_df['id'] = brand_product_df['url'].apply(extract_id)

    # Filter the DataFrame based on the desired IDs
    filtered_df = brand_product_df[brand_product_df['id'].isin(farfetch_id_list)]

    filtered_df = filtered_df.drop(columns=['id'])

    return filtered_df['url'].tolist()
def upload_file_to_space(file_src, save_as, is_public=True):
    spaces_client = get_s3_client()
    space_name = 'iconluxurygroup-s3'  # Your space name

    spaces_client.upload_file(file_src, space_name, save_as, ExtraArgs={'ACL': 'public-read'})
    # Generate and return the public URL if the file is public
    if is_public:
        # upload_url = f"{str(os.getenv('SPACES_ENDPOINT'))}/{space_name}/{save_as}"
        upload_url = f"https://iconluxurygroup-s3.s3.us-east-2.amazonaws.com/{save_as}"
        return upload_url

def get_s3_client():
    session = boto3.session.Session()
    client = boto3.client(service_name='s3',
                          region_name=os.getenv('REGION'),
                          aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    return client

def parse_farfetch_brand(job_id, brand_id,output_csv_link):
    code=uuid.uuid4()
    if brand_id==401:
        brand_product_csv=r'C:\Users\ICON\Sam\Farfetch_Parsing\FarfetchParser\URL_Input_CSVs\off-white_single_product_us.csv'
        brand_product_df=pd.read_csv(brand_product_csv)
        brand_product_df.columns=['url']
        brand='off_white'
    elif brand_id==412:
        brand_product_csv=r'C:\Users\ICON\Sam\Farfetch_Parsing\FarfetchParser\URL_Input_CSVs\palm-angels_category_us.csv'
        brand_product_df = pd.read_csv(brand_product_csv)
        brand_product_df.columns = ['url']
        brand='palm_angels'
    else:
        raise ValueError("Unsupported brand_id")

    print(brand_product_df.head(5))
    output_csv_text=open_link(serverless_urls,output_csv_link)
    product_details_df = pd.read_csv(StringIO(output_csv_text),header=0)
    farfetch_id_list = product_details_df['product_id'].astype(str).str.replace('.0', '')
    product_url_list = get_product_url_list(brand_product_df, farfetch_id_list)
    print(f"First 10 elements of product url list\n{product_url_list[:10]}")
    parser = FarfetchProductParser(product_url_list)
    product_details = parser.parse()
    product_id_df = pd.DataFrame(product_details)
    product_details_df['product_id'] = product_details_df['product_id'].astype(str)
    product_id_df['farfetch_id'] = product_id_df['farfetch_id'].astype(str)
    final_df = pd.merge(product_details_df, product_id_df, left_on='product_id', right_on='farfetch_id', how='inner')
    final_df = final_df[final_df['brand_style_id'].str.strip() != '']
    final_df['product_id'] = final_df['brand_style_id']
    final_df = final_df.drop(columns=['brand_style_id', 'farfetch_id'])
    final_df.to_csv('final_output.csv', index=False)
    filename=f'output_{brand}_{code}.csv'
    final_df.to_csv(filename, index=False)
    print(final_df)
    print(final_df.info)
    count=len(final_df.index)
    s3_output_file= upload_file_to_space(filename,filename)
    send_output(job_id,s3_output_file,count)

def send_output(job_id,s3_output_file,count):
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }

    params = {
        'job_id': f"{job_id}",
        'resultUrl': f"{s3_output_file}",
        'count': count
    }
    requests.post(f"{os.getenv('MANAGER_ENDPOINT')}/job_complete", params=params, headers=headers)

@app.post("/run_parser")
async def brand_batch_endpoint(job_id:int, brand_id:int, output_csv:str, background_tasks: BackgroundTasks):
    background_tasks.add_task(parse_farfetch_brand,job_id,brand_id,output_csv)
    return {"message": "Notification sent in the background"}


if __name__ == "__main__":
    uvicorn.run("agent_palm_offwhite:app", port=8004, log_level="info")