import json
import os
from pathlib import Path
from supabase import create_client

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

def dump_json(data, filename="data/endsars_list.json"):
    Path("data").mkdir(exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def run_scraper():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    victims_list = scrape_data()  # Assume this exists
    
    # Insert into Supabase
    supabase.table('victims').insert(victims_list).execute()
    
    # Dump to JSON after successful insert
    dump_json(victims_list)

if __name__ == "__main__":
    run_scraper()