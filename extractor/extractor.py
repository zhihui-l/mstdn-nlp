import requests
import time
import json
from datetime import datetime
import os

def fetch_timeline(api_url):
    response = requests.get(api_url)
    return response.json()

def save_data(data, directory):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{directory}/timeline_{timestamp}.json"
    with open(filename, 'w') as f:
        json.dump(data, f)

def main():
    mastodon_api_urls = [
        "https://mastodon.social/api/v1/timelines/public",
        "https://fosstodon.org/api/v1/timelines/public"
    ]
    data_directory = "/opt/datalake"

    while True:
        os.makedirs(data_directory, exist_ok=True)
        for url in mastodon_api_urls:
            data = fetch_timeline(url)
            save_data(data, data_directory)
        time.sleep(30)

if __name__ == "__main__":
    main()
