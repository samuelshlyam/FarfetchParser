import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import random
import time
class FarfetchProductParser:
    def __init__(self, html_list):
        self.html_list = html_list

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
        all_product_images = []

        for html_content in self.html_list:
            soup = BeautifulSoup(html_content, 'html.parser')
            product_details = self.parse_product_details(soup)
            all_product_details.append(product_details)

        return all_product_details
def open_link(serverless_urls,url_in):
    while True:
        payload = json.dumps({
            "url": url_in
        })
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.3',
            'Content-Type': 'application/json'
        }
        url=random.choice(serverless_urls)
        print(url)
        response = requests.request("POST", url, headers=headers, data=payload)
        response = json.loads(response.text)
        print(response.get('result'))
        time.sleep(3)
        if not ("Access Denied" in response.get('result',"Access Denied") or "429 Too Many Requests" in response.get('result',"429 Too Many Requests")):
            break
    return response.get('result')


# Example usage
file_path = r'/Users/samuelshlyam/Downloads/pythonProject (1)/us-sitemap-brands-categories-1.xml'  # Adjust the file path as needed
html_contents=[]
df = pd.read_xml(file_path)
small_df=df.iloc[:100]
serverless_urls=["https://router-proxy-google-cloud-o5empm7y3q-rj.a.run.app/fetch", "https://router-proxy-google-cloud-2-o5empm7y3q-pd.a.run.app/fetch","https://router-proxy-google-cloud-o5empm7y3q-pd.a.run.app/fetch"]
for index, row in small_df.iterrows():
    # Access data by column name
    loc_value = row['loc'] if 'loc' in row else None
    print(loc_value)
    html_content=open_link(serverless_urls,loc_value)
    html_contents.append(html_content)
clean_html_contents = list(filter(None,html_contents))

# Example usage:
parser = FarfetchProductParser(clean_html_contents)
product_details = parser.parse()
file_path_csv=r"C:\Users\User\PycharmProjects\Farfetch Parser\product_details.csv"
print("Product Details:", product_details)
product_details_df=pd.DataFrame(product_details)
product_details_df.to_csv(file_path_csv)