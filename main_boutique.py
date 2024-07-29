import os

import pandas as pd
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

current_date=datetime.now().strftime("%m_%d_%Y")
current_directory= os.getcwd()
main_directory=os.path.join(current_directory,'outputs')
class FarfetchBoutiqueParser:
    def __init__(self,url):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=options)
        self.url = url
        self.html_list=self.get_html_list()


    def parse_boutique_details(self):
        boutiques = []
        for html in self.html_list:
            soup = BeautifulSoup(html, 'html.parser')
            main_component=soup.find('div',class_='ltr-1nm4v7d')
            accordions=main_component.find_all('div',class_='ltr-161ftst e1q06tt43')
            for accordion in accordions:
                boutique={}

                name_component = accordion.find('p',class_='ltr-13a5og8-Title')
                location_component = accordion.find('p',class_='ltr-1gp3mca-Footnote')
                brands_component = accordion.find('p',class_='ltr-4y8w0i-Body')


                name = name_component.text.strip() if name_component else ''
                location = location_component.text.strip() if location_component else ''
                brands = brands_component.text.strip() if brands_component else ''

                boutique['Name'] = name
                boutique['Location'] = location
                boutique['Brands'] = brands
                boutiques.append(boutique)
        return pd.DataFrame(boutiques)
    def get_html_list(self):
        self.driver.get(self.url)
        html_list=[]
        locator_type = By.CSS_SELECTOR
        locator='button.ltr-6ope4c'
        next_button = WebDriverWait(self.driver, 5).until(
            EC.element_to_be_clickable((locator_type, locator))
        )
        condition=True
        while condition:
            try:
                html=self.driver.execute_script("return document.documentElement.outerHTML;")
                soup = BeautifulSoup(html, 'html.parser')
                self.driver.execute_script("arguments[0].click();", next_button)
                next_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((locator_type, locator))
                        )
                time.sleep(3)
                button_element=soup.find('div',class_='ltr-skqeav evqtvby0')
                html_list.append(html)
                if button_element:
                    text = button_element.find('p', class_='ltr-2pfgen-Body-BodyBold').text.strip()
                    comparison=text.split('of')
                    comparison=[element.strip() for element in comparison]
                    filename = os.path.join(main_directory,'boutique_output','output_html', f'page_{comparison[0]}_html.html')
                    self.save_html(html, filename)
                    print(comparison)
                    if comparison[0]==comparison[1]:
                        condition=False
                else:
                    continue
            except Exception as e:
                print('Final button pressed')
                print(f'Error occured {e}')
                break
        self.driver.close()
        print(f"this is the amount of html in the html list {len(html_list)}")
        return html_list
    def save_html(self,html,filename):
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(html)
            print(f"HTML content has been successfully written to {filename}")
        except IOError as e:
            print(f"An error occurred while writing to the file: {e}")



url='https://www.farfetch.com/boutiques/boutiques'
Farfetch_Boutique_Parser = FarfetchBoutiqueParser(url)
file_path_csv = os.path.join(main_directory,'boutique_output',f'output_{current_date}.csv')
output = Farfetch_Boutique_Parser.parse_boutique_details().to_csv(file_path_csv)