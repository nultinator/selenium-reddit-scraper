from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from urllib.parse import urlencode
import csv, json, time
import logging, os
from dataclasses import dataclass, field, fields, asdict
from concurrent.futures import ThreadPoolExecutor

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'
options = ChromeOptions()
options.add_argument("--headless")
options.add_argument(f"user-agent={user_agent}")

proxy_url = "https://proxy.scrapeops.io/v1/"
API_KEY = "YOUR-SUPER-SECRET-API-KEY"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchData:
    name: str = ""
    author: str = ""
    permalink: str = ""
    upvote_ratio: float = 0.0

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == '':
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())

class DataPipeline:
    
    def __init__(self, csv_filename='', storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode='a', newline='', encoding='utf-8') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()

def get_scrapeops_url(url, location="us"):
    payload = {
        "api_key": API_KEY,
        "url": url,
        "country": location
    }
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    return proxy_url

#get posts from a subreddit
def get_posts(feed, limit=100, retries=3, data_pipeline=None, location="us"):
    tries = 0
    success = False
    
    while tries <= retries and not success:
        driver = webdriver.Chrome(options=options)
        try:
            url = f"https://www.reddit.com/r/{feed}.json?limit={limit}"
            driver.get(get_scrapeops_url(url, location=location))
            json_text = driver.find_element(By.TAG_NAME, "pre").text
            resp = json.loads(json_text)

            if resp:
                success = True
                children = resp["data"]["children"]
                for child in children:
                    data = child["data"]

                    article_data = SearchData(
                        name=data["title"],
                        author=data["author"],
                        permalink=data["permalink"],
                        upvote_ratio=data["upvote_ratio"]
                    )

                    data_pipeline.add_data(article_data)
                    
                    
                    
            else:
                logger.warning(f"Failed response from server, tries left: {retries-tries}")
                raise Exception("Failed to get posts")
        except Exception as e:
            driver.save_screenshot(f"error-{tries}.png")
            logger.warning(f"Exeception, failed to get posts: {e}")
            tries += 1
        finally:
            driver.quit()
    

########### MAIN FUNCTION #############

if __name__ == "__main__":

    FEEDS = ["news"]
    BATCH_SIZE = 100

    for feed in FEEDS:
        feed_filename = feed.replace(" ", "-")
        feed_pipeline = DataPipeline(csv_filename=f"{feed_filename}.csv")
        get_posts(feed, limit=BATCH_SIZE, data_pipeline=feed_pipeline)
        feed_pipeline.close_pipeline()