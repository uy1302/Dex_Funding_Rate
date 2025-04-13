import time
import os
import re
from dotenv import load_dotenv
import pymongo
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

py_client = pymongo.MongoClient(MONGO_URI)
database = py_client["funding_rates"]
collection = database["drift"]

symbols = ["BTC-PERP", "ETH-PERP"]

def get_funding_rates_and_save():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        for symbol in symbols:
            print(f"Navigating to Drift Trade for {symbol}...")
            driver.get(f"https://app.drift.trade/{symbol}")
            
            time.sleep(10)
            
            print(f"Waiting for {symbol} funding rate element...")
            
            xpath = "/html/body/div/div/div[2]/div/div/div/div/div[2]/div/div[1]/div/div[2]/div/div[2]/div/div/span/span/div/div[2]/div/div/span/span/span"
            
            try:
                wait = WebDriverWait(driver, 20)
                element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
                
                funding_rate_text = element.text
                print(f"Current {symbol} funding rate text: {funding_rate_text}")
                
                numeric_match = re.search(r'([+-]?\d+\.\d+)%', funding_rate_text)
                if numeric_match:
                    rate_value = float(numeric_match.group(1)) / 100
                    timestamp = int(time.time())
                    
                    record = {
                        "product": symbol,
                        "fund_rate": rate_value,
                        "timestamp": timestamp
                    }
                    
                    print(record)
                    
                    collection.insert_one(record)
                    print(f"Inserted funding rate for {record['product']} into MongoDB.")
                else:
                    print(f"Could not extract numeric value from {funding_rate_text}")
   
            except Exception as e:
                print(f"Error scraping {symbol}: {str(e)}")

                try:
                    page_source = driver.page_source
                    with open(f"drift_{symbol}_page_source.html", "w", encoding="utf-8") as f:
                        f.write(page_source)
                    print(f"Page source saved to drift_{symbol}_page_source.html")
                except:
                    pass
                
                driver.save_screenshot(f"drift_{symbol}_error.png")
                print(f"Screenshot saved as drift_{symbol}_error.png")
            
            time.sleep(5)
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")

        
    finally:
        driver.quit()

if __name__ == "__main__":
    while True:
        print("Starting funding rate collection cycle...")
        get_funding_rates_and_save()
        print("Sleeping for one hour before next collection...")
        time.sleep(3600) 