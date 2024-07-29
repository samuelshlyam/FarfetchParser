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
        self.html_dict_list = self.get_category_html_list()


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
            category_dict={'category_html':category_html, 'category_url': category_url}
            category_html_list.append(category_dict)
        return category_html_list

    def get_product_html_list(self,category_html):
        soup=BeautifulSoup(category_html,'html.parser')
        products=soup.find_all('li',class_='ltr-2u1m5k')
        product_html_list=[]
        for product in products:
            product_url=product.find('a',class_='ltr-1t9m6yq').get('href','')
            product_url=f"https://www.farfetch.com{product_url}"
            product_html= self.open_link(self.serverless_urls, product_url)
            product_dict= {'product_url':product_url, 'product_html':product_html}
            product_html_list.append(product_dict)
        return product_html_list

    def parse_product_details(self, soup):
        details_section = soup.find('div', {'data-testid': 'product-information-accordion'})
        if not details_section:
            details_section = soup.find('div', {'data-component': 'TabsContainer'})

        product_info = {'sold_out': False, 'tag': '', 'brand': '', 'product_name': '', 'made_in': '', 'highlights': [],
                        'composition': {}, 'farfetch_id': '', 'brand_style_id': '', 'image_urls': '',
                        'product_url': self.current_product_url, 'category_url':self.current_category_url}

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

        for category_dict in self.html_dict_list:
            category_html=category_dict.get('category_html')
            self.current_category_url=category_dict.get('category_url')
            product_html_list=self.get_product_html_list(category_html)
            for product_dict in product_html_list:
                print(product_dict)
                product_html_content=product_dict.get('product_html')
                self.current_product_url=product_dict.get('product_url')
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

            response = requests.request("POST", url, headers=headers, data=payload)
            response = json.loads(response.text)
            print(f"Current serverless URL: {url}")
            print(f"Trying to open {url_in}")
            print(response.get('result')[:1000])
            time.sleep(3)
            if not ("Access Denied" in response.get('result',"Access Denied") or "429 Too Many Requests" in response.get('result',"429 Too Many Requests")):
                break

        return response.get('result')


import csv
from urllib.parse import unquote

import csv
from urllib.parse import unquote
import os


def extract_and_categorize_urls(input_csv, output_dir, brands,countries):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Initialize dictionaries to store file handles and CSV writers
    file_handles = {}
    csv_writers = {}

    # Create CSV files for each brand and category
    for brand in brands:
        for category in ['category', 'single_product']:
            for country in countries:
                temp_country = country.replace('/shopping', '')
                temp_country = 'us' if temp_country == '.com' else temp_country
                filename = f"{brand}_{category}_{temp_country}.csv"
                file_path = os.path.join(output_dir, filename)
                file_handles[f"{brand}_{category}_{temp_country}"] = open(file_path, 'w', newline='', encoding='utf-8')
                csv_writers[f"{brand}_{category}_{temp_country}"] = csv.writer(file_handles[f"{brand}_{category}_{temp_country}"])
                csv_writers[f"{brand}_{category}_{temp_country}"].writerow(['URL'])  # Write header

    with open(input_csv, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        next(reader, None)  # Skip header

        for row in reader:
            if row:  # Check if the row is not empty
                url = unquote(row[0]).lower()  # Get the URL from the first column, decode it, and convert to lowercase

                for brand in brands:
                    for country in countries:
                        if brand in url and country in url:
                            category = 'category' if url.endswith('items.aspx') else 'single_product'
                            temp_country=country.replace('/shopping','')
                            temp_country='us' if temp_country=='.com' else temp_country
                            csv_writers[f"{brand}_{category}_{temp_country}"].writerow([url])
                            break  # Stop checking other brands once a match is found

    # Close all file handles
    for file_handle in file_handles.values():
        file_handle.close()

    print(f"Extraction and categorization complete. Results saved in {output_dir}")


def get_urls_from_csv(csv_filename):
    urls = []

    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # Skip the header row

            for row in reader:
                if row:  # Check if the row is not empty
                    url = unquote(row[0]).strip()  # Get the URL from the first column, decode it, and remove whitespace
                    if url:  # Check if the URL is not empty after stripping
                        urls.append(url)

    except FileNotFoundError:
        print(f"Error: The file '{csv_filename}' was not found.")
    except csv.Error as e:
        print(f"Error reading CSV file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return urls
#Get csv full of input URL's (should use category for now)
# brands = ['off-white', 'palm-angels']  # This list can be of any length
# countries = ['uk/shopping','it/shopping','.com/shopping']
# extract_and_categorize_urls('farfetch_urls.csv', 'URL_Input_CSVs', brands,countries)


# Example usage:
brand_name='off-white'
country_name='it'
input_csv_filename = os.path.join(current_directory, 'URL_Input_CSVs', f'{brand_name}_category_{country_name}.csv')
initial_base_urls=get_urls_from_csv(input_csv_filename)
base_urls=[initial_base_url+'?page={page}' for initial_base_url in initial_base_urls]
all_product_details=[]
for base_url in base_urls:
    parser = FarfetchProductParser(base_url)
    product_details = parser.parse()
    all_product_details.extend(product_details)
file_path_csv=os.path.join(main_directory,'product_output','product_output.csv')
print("Product Details:", all_product_details)
product_details_df=pd.DataFrame(all_product_details)
product_details_df.to_csv(file_path_csv)
