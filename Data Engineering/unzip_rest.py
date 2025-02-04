import zipfile
import os

# Path to the restaurant zip file
zip_path = r'c:/Users/user/dilrabo/master-data/hotle/restaurant_csv.zip'

# Directory to extract to (same directory as the zip file)
extract_path = r'c:/Users/user/dilrabo/master-data/hotle'

# Ensure the extraction directory exists
os.makedirs(extract_path, exist_ok=True)

# Open the zip file and extract
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print(f"Successfully extracted {zip_path} to {extract_path}")