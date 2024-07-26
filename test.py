import requests
import json
import random
import time


class URLFetcher:
    def __init__(self, serverless_urls):
        self.serverless_urls = serverless_urls

    @staticmethod
    def open_link(serverless_urls, url_in):
        max_retries = 5
        retry_delay = 3

        for attempt in range(max_retries):
            payload = json.dumps({
                "url": url_in
            })
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.3',
                'Content-Type': 'application/json'
            }
            url = random.choice(serverless_urls)
            print(f"Attempt {attempt + 1}, Using serverless URL: {url}")

            try:
                response = requests.post(url, headers=headers, data=payload, timeout=30)
                response.raise_for_status()  # Raise an exception for bad status codes
                result = response.json().get('result', '')

                print(f"Response preview: {result[:1000]}")

                if "Access Denied" not in result and "429 Too Many Requests" not in result and 'Example Domain' not in result:
                    return result

                print(f"Encountered issue: {'Access Denied' if 'Access Denied' in result else '429 Too Many Requests'}")
            except requests.RequestException as e:
                print(f"Request failed: {str(e)}")

            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

        raise Exception("Max retries reached. Unable to fetch the URL.")


# Example usage
serverless_urls = [
    "https://router-proxy-google-cloud-2-o5empm7y3q-pd.a.run.app/fetch",
    "https://router-proxy-google-cloud-o5empm7y3q-pd.a.run.app/fetch"
]

fetcher = URLFetcher(serverless_urls)

try:
    result = fetcher.open_link(serverless_urls, "https://www.example.com")
    print("Successfully fetched the URL")
    print(f"Result preview: {result[:500]}")  # Print first 500 characters of the result
except Exception as e:
    print(f"Failed to fetch the URL: {str(e)}")