import json
import os
from pathlib import Path

def dump_json(data, filename="data/endsars_list.json"):
    Path("data").mkdir(exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

# Call dump_json(victims_list) after Supabase insert
