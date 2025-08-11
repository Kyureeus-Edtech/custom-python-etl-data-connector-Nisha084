from utils.extractor import get_blacklist, check_ip
from utils.transformer import transform_ip_data
from utils.loader import insert_many
from pymongo import MongoClient
from dotenv import load_dotenv
import os

def run_pipeline():
    print("[Info] Starting AbuseIPDB ETL pipeline...")


    # --- Clear old data before inserting new ---

    '''
    load_dotenv()

    client = MongoClient(os.getenv("MONGODB_URI"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[os.getenv("MONGO_COLLECTION_NAME")]
    collection.delete_many({})
    print(f"[Info] Cleared existing documents in {collection.name}")
    '''
    
    # ------------------------------------------
    
    # Extract
    blacklist_data = get_blacklist(limit=5)  # adjust limit as needed
    ip_list = [item["ipAddress"] for item in blacklist_data]

    all_ip_details = []
    for ip in ip_list:
        print(f"[Info] Checking IP: {ip}")
        details = check_ip(ip)
        transformed = transform_ip_data(details)
        if transformed:
            all_ip_details.append(transformed)

    # Load
    insert_many(all_ip_details)
    print("[Info] Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
