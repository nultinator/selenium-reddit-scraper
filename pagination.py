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

#get posts from a subreddit
def get_posts(feed, limit=100, retries=3):
    tries = 0
    success = False
    
    while tries <= retries and not success:
        driver = webdriver.Chrome(options=options)
        try:
            url = f"https://www.reddit.com/r/{feed}.json?limit={limit}"
            driver.get(url)
            time.sleep(1)
            json_text = driver.find_element(By.TAG_NAME, "pre").text
            resp = json.loads(json_text)

            if resp:
                success = True
                children = resp["data"]["children"]
                for child in children:
                    data = child["data"]
                    
                    #extract individual fields from the site data
                    name = data["title"]
                    author = data["author_fullname"]
                    permalink = data["permalink"]
                    upvote_ratio = data["upvote_ratio"]

                    #print the extracted data
                    print(f"Name: {name}")
                    print(f"Author: {author}")
                    print(f"Permalink: {permalink}")
                    print(f"Upvote Ratio: {upvote_ratio}")
                    
            else:
                logger.warning(f"Failed response: {resp.status_code}")
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
    BATCH_SIZE = 2

    for feed in FEEDS:
        get_posts(feed, limit=BATCH_SIZE)