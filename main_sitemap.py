import requests
import json
import random
import time
import gzip
import xml.etree.ElementTree as ET
import csv
from lxml import html

class SitemapProcessor:
    def __init__(self, sitemap_index_url, max_retries=10, initial_delay=5, rate_limit_delay=2):
        self.sitemap_index_url = sitemap_index_url
        self.namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        })

    def download_with_retry(self, url):
        for attempt in range(self.max_retries):
            try:
                print(f"Attempt {attempt + 1} to download {url}")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                time.sleep(self.rate_limit_delay)  # Rate limiting
                return response
            except requests.RequestException as e:
                if response.status_code in [403, 429]:
                    delay = self.initial_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"{response.status_code} error. Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Error downloading {url}: {str(e)}")
                    return None
        print(f"Max retries reached for {url}")
        return None

    def check_robots_txt(self):
        robots_url = 'https://www.farfetch.com/robots.txt'
        try:
            response = self.session.get(robots_url, timeout=30)
            response.raise_for_status()
            print("Successfully accessed robots.txt")
            print("First 500 characters of robots.txt:")
            print(response.text[:500])
            return True
        except requests.RequestException as e:
            print(f"Error accessing robots.txt: {str(e)}")
            return False

    def download_and_parse_xml(self, url):
        print(f"Downloading and parsing XML from {url}")
        start_time = time.time()

        response = self.download_with_retry(url)
        if not response:
            return None

        print(f"Download completed in {time.time() - start_time:.2f} seconds")

        content_type = response.headers.get('Content-Type', '')
        if 'application/x-gzip' in content_type or url.endswith('.gz'):
            print("Content is gzipped. Decompressing...")
            try:
                content = gzip.decompress(response.content)
            except gzip.BadGzipFile:
                print("Failed to decompress GZIP content")
                return None
        else:
            content = response.content

        print(f"Content length: {len(content)} bytes")
        print("First 500 characters of content:")
        print(content[:500])
        print("\n")

        try:
            print("Parsing XML...")
            return ET.fromstring(content)
        except ET.ParseError as e:
            print(f"XML parsing error: {str(e)}")
            print("Attempting to parse as HTML...")
            try:
                tree = html.fromstring(content)
                if tree.xpath('//html'):
                    print("The response appears to be HTML, not XML.")
            except Exception as html_e:
                print(f"HTML parsing error: {str(html_e)}")
            return None

    def extract_urls_from_sitemap_index(self):
        print("Extracting URLs from sitemap index...")
        root = self.download_and_parse_xml(self.sitemap_index_url)
        if root is None:
            print("Failed to parse sitemap index")
            return []
        urls = [loc.text for loc in root.findall('.//sm:loc', self.namespace)]
        print(f"Found {len(urls)} URLs in sitemap index")
        return urls

    def extract_urls_from_sitemap(self, sitemap_url):
        print(f"Extracting URLs from sitemap: {sitemap_url}")
        root = self.download_and_parse_xml(sitemap_url)
        if root is None:
            print(f"Failed to parse sitemap: {sitemap_url}")
            return []
        urls = [loc.text for loc in root.findall('.//sm:loc', self.namespace)]
        print(f"Found {len(urls)} URLs in this sitemap")
        return urls

    def get_urls(self):
        all_page_urls = []
        print("Starting to process sitemap index...")
        sitemap_urls = self.extract_urls_from_sitemap_index()

        for i, sitemap_url in enumerate(sitemap_urls, 1):
            print(f"Processing sitemap {i} of {len(sitemap_urls)}: {sitemap_url}")
            try:
                page_urls = self.extract_urls_from_sitemap(sitemap_url)
                for page_url in page_urls:
                    all_page_urls.append({
                        'url': page_url,
                        'parent_sitemap': sitemap_url,
                        'sitemap_index': self.sitemap_index_url
                    })
                print(f"Total URLs extracted so far: {len(all_page_urls)}")
            except Exception as e:
                print(f"Error processing sitemap {sitemap_url}: {str(e)}")

        return all_page_urls


def save_to_csv(data, base_filename):
    max_file_size = 500 * 1024 * 1024  # 500 MB in bytes
    fieldnames = ['url', 'parent_sitemap', 'sitemap_index']
    file_counter = 1
    current_size = 0
    current_file = None
    writer = None

    for row in data:
        if current_file is None or current_size >= max_file_size:
            if current_file:
                current_file.close()
                print(f"Data saved to {current_filename}")

            current_filename = f"{base_filename}_{file_counter}.csv"
            current_file = open(current_filename, 'w', newline='', encoding='utf-8')
            writer = csv.DictWriter(current_file, fieldnames=fieldnames)
            writer.writeheader()
            current_size = 0
            file_counter += 1

        row_size = sum(len(str(value).encode('utf-8')) for value in row.values())
        if current_size + row_size > max_file_size:
            current_file.close()
            print(f"Data saved to {current_filename}")

            current_filename = f"{base_filename}_{file_counter}.csv"
            current_file = open(current_filename, 'w', newline='', encoding='utf-8')
            writer = csv.DictWriter(current_file, fieldnames=fieldnames)
            writer.writeheader()
            current_size = 0
            file_counter += 1

        writer.writerow(row)
        current_size += row_size

    if current_file:
        current_file.close()
        print(f"Data saved to {current_filename}")

    print(f"Total files created: {file_counter - 1}")

# Get URLs from sitemap
sitemap_index_url = "https://www.farfetch.com/sitemap.xml"
processor = SitemapProcessor(sitemap_index_url)

try:
    print("Starting URL extraction process...")
    start_time = time.time()
    urls = processor.get_urls()
    end_time = time.time()
    print(f"URL extraction completed in {end_time - start_time:.2f} seconds")
    print(f"Total URLs extracted: {len(urls)}")
    print("First 5 URLs:")
    for url in urls[:5]:
        print(url)

    # Save to CSV
    save_to_csv(urls, 'farfetch_urls.csv')
except Exception as e:
    print(f"An error occurred: {str(e)}")
# Get URLs from sitemap
sitemap_index_url = "https://www.farfetch.com/sitemap.xml"
processor = SitemapProcessor(sitemap_index_url)

try:
    print("Starting URL extraction process...")
    start_time = time.time()
    urls = processor.get_urls()
    end_time = time.time()
    print(f"URL extraction completed in {end_time - start_time:.2f} seconds")
    print(f"Total URLs extracted: {len(urls)}")
    print("First 5 URLs:")
    for url in urls[:5]:
        print(url)

    # Save to CSV
    save_to_csv(urls, 'farfetch_urls.csv')
except Exception as e:
    print(f"An error occurred: {str(e)}")
# # Get URLs from sitemap
# sitemap_index_url = "https://www.farfetch.com/sitemap.xml"
# processor = SitemapProcessor(sitemap_index_url)
#
# try:
#     print("Starting URL extraction process...")
#     start_time = time.time()
#     urls = processor.get_urls()
#     end_time = time.time()
#     print(f"URL extraction completed in {end_time - start_time:.2f} seconds")
#     print(f"Total URLs extracted: {len(urls)}")
#     print("First 5 URLs:")
#     for url in urls[:5]:
#         print(url)
#
#     # Save to CSV
#     save_to_csv(urls, 'farfetch_urls.csv')
# except Exception as e:
#     print(f"An error occurred: {str(e)}")
