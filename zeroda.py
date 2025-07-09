import os
import json
import time
import schedule
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Constants
URL = "https://pulse.zerodha.com/"
CSV_FILE = "pulse_news.csv"
JSON_FILE = "pulse_news.json"
SCRAPE_INTERVAL_MIN = 30  # customize interval here

# Setup headless Chrome
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# Load already scraped data (for de-duplication)
def load_existing_data():
    if os.path.exists(CSV_FILE):
        return pd.read_csv(CSV_FILE)
    return pd.DataFrame(columns=["tag", "title", "description", "timestamp", "source", "url"])

# Save to CSV and JSON
def save_data(new_data, all_data):
    all_data.to_csv(CSV_FILE, index=False)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data.to_dict(orient="records"), f, indent=2, ensure_ascii=False)

# Extract article info from HTML
def parse_articles(soup, tag_name):
    articles = []
    for item in soup.select("ul#news li.item"):
        try:
            title_tag = item.select_one("h2.title a")
            desc_tag = item.select_one("div.desc")
            time_tag = item.select_one("span.date")
            source_tag = item.select_one("span.feed")

            articles.append({
                "tag": tag_name,
                "title": title_tag.get_text(strip=True),
                "description": desc_tag.get_text(strip=True) if desc_tag else "",
                "timestamp": time_tag.get("title") if time_tag else "",
                "source": source_tag.get_text(strip=True).replace("— ", "") if source_tag else "",
                "url": title_tag.get("href")
            })
        except Exception as e:
            continue
    return articles

# Scrape logic
def scrape():
    print(f"[{datetime.now()}] Starting scrape...")
    driver = get_driver()
    driver.get(URL)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    tags = [a.get("data-word") for a in soup.select("#wordcloud a")]
    tags.insert(0, "trending")  # Ensure "Trending" is first

    existing_df = load_existing_data()
    new_entries = []

    for tag in tags:
        # Click tag link if not 'trending'
        if tag != "trending":
            try:
                link = driver.find_element(By.CSS_SELECTOR, f"a[data-word='{tag}']")
                driver.execute_script("arguments[0].click();", link)
                time.sleep(2)
            except:
                continue

        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = parse_articles(soup, tag)

        for article in articles:
            if not existing_df[
                (existing_df['url'] == article['url']) &
                (existing_df['timestamp'] == article['timestamp'])
            ].empty:
                continue  # skip duplicate
            new_entries.append(article)

    driver.quit()

    if new_entries:
        new_df = pd.DataFrame(new_entries)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        save_data(new_df, combined_df)
        print(f"[+] Saved {len(new_entries)} new articles.")
    else:
        print("[=] No new articles found.")

# Scheduling
schedule.every(SCRAPE_INTERVAL_MIN).minutes.do(scrape)

if __name__ == "__main__":
    scrape()  # Initial scrape
    print(f"[i] Scheduled scraping every {SCRAPE_INTERVAL_MIN} minutes...\n")
    while True:
        schedule.run_pending()
        time.sleep(60)
