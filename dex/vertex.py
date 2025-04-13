import time
import os
from dotenv import load_dotenv
from vertex_protocol.client import create_vertex_client
import pymongo

load_dotenv()
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
MONGO_URI = os.getenv("MONGODB_URI")

client = create_vertex_client("mainnet", PRIVATE_KEY)
py_client = pymongo.MongoClient(MONGO_URI)
database = py_client["funding_rates"]
collection = database["vertex"]


product_mapping = {
    '2': 'BTC-PERP',
    '4': 'ETH-PERP',
}

def fetch_and_save_funding_rates():
    funding_rate_data = client.market.get_perp_funding_rates([2, 4])
    
    formatted = {}
    for prod_id, fund_rate in funding_rate_data.items():
        rate_value = float(fund_rate.funding_rate_x18) / 1e18  
        timestamp = int(fund_rate.update_time)
        symbol = product_mapping.get(str(prod_id), f"Unknown({prod_id})")
        formatted[symbol] = {
            "product": symbol,
            "fund_rate": rate_value,
            "timestamp": timestamp
        }
    
    for product in formatted:
        record = formatted[product]
        print(record)
        collection.insert_one(record)
        print(f"Inserted funding rate for {record['product']} into MongoDB.")

if __name__ == "__main__":
    while True:
        fetch_and_save_funding_rates()
        time.sleep(3600)
