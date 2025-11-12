"""Write existing checkpoint data to CSV and Excel files."""

import json
from pathlib import Path

from src.incremental_writer import IncrementalWriter

# Load checkpoint data
checkpoint_path = Path("data/checkpoints/checkpoint.json")
with open(checkpoint_path, 'r', encoding='utf-8') as f:
    checkpoint_data = json.load(f)

scraped_data = checkpoint_data.get("data", [])
completed_ids = checkpoint_data.get("completed_ids", [])

print(f"Loaded {len(scraped_data)} rows from checkpoint")
print(f"Completed {len(completed_ids)} items")

# Initialize writer
writer = IncrementalWriter(
    output_dir=Path("output"),
    csv_filename="mankan_nutritional_data.csv",
    excel_filename="mankan_nutritional_data.xlsx",
)

# Write all data
if scraped_data:
    print(f"Writing {len(scraped_data)} rows to CSV and Excel...")
    writer.add_data(scraped_data)
    writer.finalize()
    print("Done! Files created in output/ directory")
else:
    print("No data to write")

