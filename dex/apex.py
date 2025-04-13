import requests
import pymongo
import os
from dotenv import load_dotenv
import time

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

py_client = pymongo.MongoClient(MONGO_URI)
database = py_client["funding_rates"]
collection = database["apex"]

product_mapping = {
    'BTC-USDT': 'BTC-PERP',
    'ETH-USDT': 'ETH-PERP',
}

def get_funding_rate_history(symbol="BTC-USDT", limit=100, begin_time=None, end_time=None, page=0):
    base_url = "https://omni.apex.exchange/api/v3/history-funding"
    params = {
        "symbol": symbol,
        "limit": limit,
        "page": page
    }
    if begin_time is not None:
        params["beginTimeInclusive"] = begin_time
    if end_time is not None:
        params["endTimeExclusive"] = end_time
    
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Lỗi {response.status_code}: {response.text}")

def fetch_all_historical_data():
    """Fetch all available historical data by paging through results"""
    for original_symbol, mapped_symbol in product_mapping.items():
        page = 0
        total_processed = 0
        
        while page < 15:
            try:
                print(f"Fetching page {page} for {original_symbol}")
                funding_data = get_funding_rate_history(symbol=original_symbol, limit=100, page=page)
                
                if funding_data and "data" in funding_data and "historyFunds" in funding_data["data"]:
                    history_funds = funding_data["data"]["historyFunds"]
                    
                    if not history_funds:
                        print(f"No more data for {original_symbol} after page {page}")
                        break
                    
                    for funding_entry in history_funds:
                        record = {
                            "product": mapped_symbol,
                            "fund_rate": float(funding_entry["rate"]),
                            "timestamp": int(funding_entry["fundingTimestamp"]) // 1000  
                        }
                        
                        collection.insert_one(record)
                    
                    total_processed += len(history_funds)
                    print(f"Processed {len(history_funds)} entries on page {page} for {original_symbol}")
                    
                    if len(history_funds) < 100:
                        print(f"Reached end of data for {original_symbol}")
                        break
                    
                    page += 1
                    
                    time.sleep(0.5)
                    
                else:
                    print(f"No funding data available for {original_symbol} on page {page}")
                    break
                    
            except Exception as e:
                print(f"Error processing {original_symbol} on page {page}: {e}")
                break
        
        print(f"Finished processing {original_symbol}. Total entries: {total_processed}")

if __name__ == "__main__":
    fetch_all_historical_data()