# Spark ETL Pipeline for Restaurant and Weather Data

## Project Overview
This project implements a Spark-based ETL (Extract, Transform, Load) pipeline to process and enrich restaurant and weather data.

## Data Integration Workflow

### Data Sources
1. **Restaurant Dataset** (`restaurant.csv`):
   - Contains restaurant information
   - Columns: 
     - `restaurant_id`: Unique identifier
     - `name`: Restaurant name
     - `latitude`: Geographical latitude (may contain null values)
     - `longitude`: Geographical longitude (may contain null values)

2. **Weather Dataset** (`weather.csv`):
   - Contains weather information for specific geographical areas
   - Columns:
     - `geohash`: 4-character geohash representing a geographical region
     - `temperature`: Temperature in the region
     - `precipitation`: Precipitation amount

### Data Integration Process
1. **Data Loading and Validation**
   - Load restaurant and weather datasets
   - Validate schema and check for null coordinates

2. **Geocoding**
   - For restaurants with null latitude/longitude
   - Use OpenCage Geocoding API to retrieve missing coordinates
   - Enriches dataset with accurate geographical information

3. **Geohash Generation**
   - Generate 4-character geohash for each restaurant location
   - Uses `pygeohash` library for precise geohash creation

4. **Data Joining**
   - Perform left join between restaurant and weather datasets
   - Join key: 4-character geohash
   - Prevents data multiplication
   - Preserves all restaurant records

5. **Data Storage**
   - Save enriched dataset in Parquet format
   - Maintain data partitioning for efficient querying

## Prerequisites
- Python 3.8+
- Apache Spark 3.3.2
- Java JDK
- Hadoop Winutils (for Windows)

## Installation
```bash
# Create virtual environment
python -m venv sparkenv
source sparkenv/bin/activate  # On Windows: sparkenv\Scripts\activate

# Install dependencies
pip install -r requirements.txt