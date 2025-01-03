## 🌍 Project Overview
This Apache Spark-based ETL (Extract, Transform, Load) project enriches restaurant data through a comprehensive geospatial and weather analysis pipeline. The project demonstrates advanced data processing techniques using PySpark.

### 🚀 Key Features
- Geocoding restaurant addresses with OpenCage API
- Generating 4-character geohashes for spatial indexing
- Left-joining restaurant and weather datasets
- Handling missing geospatial data
- Storing enriched data in Parquet format

## 🛠 Prerequisites

### System Requirements
- Python 3.8+
- Java 11 or Java 17
- Apache Spark 3.4.1
- OpenCage Geocoding API Key

### 📦 Dependencies
- pyspark
- python-dotenv
- requests
- geohash2
- pytest

## 🔧 Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/spark-sample.git
cd spark-sample