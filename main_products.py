import os
import csv
import pandas as pd
from bs4 import BeautifulSoup
import json
import random
import time
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import gzip
from urllib.parse import urlparse
current_date=datetime.now().strftime("%m_%d_%Y")
current_directory= os.getcwd()
main_directory=os.path.join(current_directory,'outputs')



class FarfetchProductParser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.serverless_urls=["https://router-proxy-google-cloud-2-o5empm7y3q-pd.a.run.app/fetch","https://router-proxy-google-cloud-o5empm7y3q-pd.a.run.app/fetch"]
        self.total_pages = self.get_total_pages()
        self.category_url_list=self.get_category_url_list()
        self.html_list = self.get_category_html_list()


    def get_total_pages(self):
        current_url=self.base_url.format(page=0)
        temp_html_content=self.open_link(self.serverless_urls,current_url)
        soup=BeautifulSoup(temp_html_content,'html.parser')
        total_pages_element=soup.find('span', attrs={
                                        'data-component': 'PaginationLabel',
                                        'class': 'ltr-gq26dl'
                                    })
        print(total_pages_element)

        total_pages_text=total_pages_element.text.strip()
        print(total_pages_text)

        total_pages_list = []
        if 'de' in total_pages_text:
            total_pages_list = total_pages_text.split("de") if total_pages_element else []
        elif 'of' in total_pages_text:
            total_pages_list=total_pages_text.split("of") if total_pages_element else []

        total_pages_list=[x.strip() for x in total_pages_list]
        print(total_pages_list)
        if total_pages_list:
            if len(total_pages_list)>1:
                total_pages=total_pages_list[1]
                print(total_pages)
                return int(total_pages)
            else:
                return 1
        else:
            return 1

    def get_category_url_list(self):
        category_url_list=[]
        for page in range(self.total_pages):
            category_url=self.base_url.format(page=page)
            category_url_list.append(category_url)
        print(category_url_list)
        return category_url_list
    def get_category_html_list(self):
        category_html_list=[]
        for category_url in self.category_url_list:
            category_html=self.open_link(self.serverless_urls,category_url)
            print(f"Working on getting category html list:\nFirst 1000 chars: {category_html[:1000]}\nCurrent url:{category_url}")

            category_html_list.append(category_html)
        return category_html_list
    def get_product_html_list(self,category_html):
        soup=BeautifulSoup(category_html,'html.parser')
        products=soup.find_all('li',class_='ltr-2u1m5k')
        product_html_list=[]
        for product in products:
            product_url=product.find('a',class_='ltr-1t9m6yq').get('href','')
            product_url=f"https://www.farfetch.com{product_url}"
            product_html= self.open_link(self.serverless_urls, product_url)
            product_html_list.append(product_html)
        return product_html_list
    def parse_product_details(self,soup):
        details_section = soup.find('div', {'data-testid': 'product-information-accordion'})
        if not details_section:
            details_section = soup.find('div', {'data-component': 'TabsContainer'})

        product_info = {
            'sold_out': False,
            'tag':'',
            'brand': '',
            'product_name': '',
            'made_in': '',
            'highlights': [],
            'composition': {},
            'farfetch_id': '',
            'brand_style_id': '',
            'image_urls':''
        }

        if details_section:
            #Extract tag
            tag_element=details_section.find('p', {'class':'ltr-xkwp1l-Body'})
            product_info['tag'] = tag_element.get_text(strip=True) if tag_element else ''

            # Extract brand
            brand_element = details_section.find('a', {'data-component': 'HeadingBold'})
            product_info['brand'] = brand_element.get_text(strip=True) if brand_element else ''

            # Extract product name
            product_name_element = details_section.find('p', {'class': 'ltr-4y8w0i-Body'})
            product_info['product_name'] = product_name_element.get_text(strip=True) if product_name_element else ''

            # Extract made in information
            made_in_element = details_section.find('div', {'class': 'ltr-jeopbd'})
            if made_in_element:
                made_in_text = made_in_element.find('p', {'class': 'ltr-4y8w0i-Body'})
                product_info['made_in'] = made_in_text.get_text(strip=True) if made_in_text else ''

            # Extract highlights
            highlights_element = details_section.find('ul', {'class': '_fdc1e5'})
            if highlights_element:
                highlights = highlights_element.find_all('li', {'class': 'ltr-4y8w0i-Body'})
                product_info['highlights'] = [highlight.get_text(strip=True) for highlight in highlights]

            # Extract composition
            composition_elements = details_section.find_all('div', {'class': 'ltr-92qs1a'})
            for comp in composition_elements:
                heading = comp.find('h4', {'data-component': 'BodyBold'}).get_text(strip=True)
                if heading == 'Composition':
                    composition_texts = comp.find_all('p', {'class': 'ltr-4y8w0i-Body'})
                    for comp_text in composition_texts:
                        parts = comp_text.get_text(strip=True).split(': ')
                        if len(parts) == 2:
                            product_info['composition'][parts[0].strip()] = parts[1].strip()
                elif heading == 'Product IDs':
                    product_ids = comp.find_all('p', {'class': 'ltr-4y8w0i-Body'})
                    for pid in product_ids:
                        if 'FARFETCH ID:' in pid.get_text():
                            product_info['farfetch_id'] = pid.find('span').get_text(strip=True)
                        if 'Brand style ID:' in pid.get_text():
                            product_info['brand_style_id'] = pid.find('span').get_text(strip=True)
            # Extract Images
            product_info['image_urls'] = self.parse_product_images(soup)

        # Check if the product is sold out
        sold_out_section = soup.find('div', {'data-component': 'TabsContainer'})
        if sold_out_section:
            product_info['sold_out'] = True

        return product_info


    def parse_product_images(self,soup):
        image_urls = []

        # Check for regular images
        images_section = soup.find('div', {'class': 'ltr-fiweo0'})
        if images_section:
            image_elements = images_section.find_all('img', {'class': 'ltr-1w2up3s'})
            image_urls.extend([img.get('src') for img in image_elements])

        # Check for sold-out images
        sold_out_images_section = soup.find('div', class_='ltr-10wu6ro eiisy9x3')
        if sold_out_images_section:
            image_elements = sold_out_images_section.find_all('img', {'class': 'ltr-1w2up3s'})
            image_urls.extend([img.get('src') for img in image_elements])

        return image_urls
    def parse(self):
        all_product_details = []

        for category_html in self.html_list:
            product_html_list=self.get_product_html_list(category_html)
            for product_html_content in product_html_list:
                print(product_html_content)
                soup = BeautifulSoup(product_html_content, 'html.parser')
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
            print(url)
            response = requests.request("POST", url, headers=headers, data=payload)
            response = json.loads(response.text)
            print(response.get('result')[:1000])
            time.sleep(3)
            if not ("Access Denied" in response.get('result',"Access Denied") or "429 Too Many Requests" in response.get('result',"429 Too Many Requests")):
                break

        return response.get('result')




# # Example usage
# html_contents=[]
#
# # Example usage:
# initial_base_url='https://www.farfetch.com/shopping/women/off-white/items.aspx'
# base_url=initial_base_url+'?page={page}'
# parser = FarfetchProductParser(base_url)
# product_details = parser.parse()
# file_path_csv=os.path.join(main_directory,'product_output','product_output.csv')
# print("Product Details:", product_details)
# product_details_df=pd.DataFrame(product_details)
# product_details_df.to_csv(file_path_csv)
