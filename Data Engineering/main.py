import zipfile
import os

# Path to the zip file
zip_path = r'c:/Users/user/dilrabo/master-data/hotle/9_August_1-4 (1).zip'

# Directory to extract to (same directory as the zip file)
extract_path = r'c:/Users/user/dilrabo/master-data/hotle'

# Open the zip file and extract
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print(f"Successfully extracted {zip_path} to {extract_path}")