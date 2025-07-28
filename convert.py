import pandas as pd
import csv
import os
excel_dir = "./excel/"
csv_dir = "./csv/"
# Loop through all files in the current directory
for filename in os.listdir(excel_dir):
    # Check if it's an Excel file and doesn't start with '~' (to skip temp files)
    if filename.endswith('.xlsx') and not filename.startswith('~'):
        # Build the output CSV file name
        base_name = os.path.splitext(filename)[0]
        csv_file = csv_dir + base_name + '.csv'
        excel_file = excel_dir + filename
        # Read the first sheet of the Excel file
        df = pd.read_excel(excel_file, sheet_name=0)

        # Insert the src_file as the first column
        src_file=''.join(base_name.split())
        df.insert(0, 'source', src_file)
        # Write to CSV with all fields quoted
        df.to_csv(
            csv_file,
            index=False,
            quoting=csv.QUOTE_ALL,
            quotechar='"',
            sep=','
        )
        print(f"Converted: {excel_file} → {csv_file}")
