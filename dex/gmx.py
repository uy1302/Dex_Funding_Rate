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
collection = database["gmx"]


symbols = ["BTC"]
symbol_mapping = {
    "BTC": "BTC-PERP",
}

def get_funding_rates_and_save():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print("Navigating to GMX trading page...")
        driver.get("https://app.gmx.io/#/trade")
        
        time.sleep(15)  
        
        try:
            accept_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'I understand')]")
            for button in accept_buttons:
                button.click()
                time.sleep(2)
            print("Clicked on accept buttons if any were found")
        except Exception as e:
            print(f"No accept buttons found or error clicking them: {e}")
        
        for symbol in symbols:
            try:
                print(f"Getting funding rate for {symbol}...")
                
                try:
                    dropdown = driver.find_element(By.XPATH, "//div[contains(@class, 'TokenSelector') or contains(@class, 'dropdown')]")
                    dropdown.click()
                    time.sleep(2)
                    
                    symbol_option = driver.find_element(By.XPATH, f"//div[contains(text(), '{symbol}')]")
                    symbol_option.click()
                    time.sleep(5)  
                    print(f"Selected {symbol} from dropdown")
                except Exception as e:
                    print(f"Could not select {symbol} from dropdown: {e}")
                
                xpath = "/html/body/div[1]/div/div[1]/div/div/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[5]/div[2]/div[1]/span"
                wait = WebDriverWait(driver, 20)
                element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
                
                funding_rate_text = element.text
                print(f"Current {symbol} funding rate text: {funding_rate_text}")
                
                numeric_match = re.search(r'([+-]?\d+\.?\d*)%', funding_rate_text)
                if numeric_match:
                    rate_value = float(numeric_match.group(1)) / 100
                    timestamp = int(time.time())
                    product = symbol_mapping.get(symbol, symbol)
                    
                    record = {
                        "product": product,
                        "fund_rate": rate_value,
                        "timestamp": timestamp
                    }
                    
                    print(record)
                    
                    collection.insert_one(record)
                    print(f"Inserted funding rate for {record['product']} into MongoDB.")
                else:
                    print(f"Could not extract numeric value from {funding_rate_text}")
                    
    
                
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")

            time.sleep(5)
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")

        
    finally:
        driver.quit()

if __name__ == "__main__":
    while True:
        print("Starting GMX funding rate collection cycle...")
        get_funding_rates_and_save()
        print("Sleeping for one hour before next collection...")
        time.sleep(3600) 