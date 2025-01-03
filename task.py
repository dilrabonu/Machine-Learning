import os
import sys
import logging
import requests
import geohash2
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, DoubleType
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session
    """
    return SparkSession.builder \
        .appName("Spark ETL Task") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def fetch_geocoding(address: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch latitude and longitude for an address using OpenCage Geocoding API
    """
    api_key = os.getenv("OPENCAGE_API_KEY")
    if not api_key:
        raise ValueError("OpenCage API key is not set in the environment variables")

    try:
        response = requests.get(
            f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={api_key}",
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        if data['results']:
            location = data['results'][0]['geometry']
            return location['lat'], location['lng']
        return None, None
    except requests.RequestException as e:
        logger.error(f"Error fetching geocoding for address '{address}': {e}")
        return None, None

def generate_geohash(lat: float, lon: float, precision: int = 4) -> Optional[str]:
    """
    Generate a geohash for the given latitude and longitude
    """
    try:
        return geohash2.encode(lat, lon, precision=precision)
    except Exception as e:
        logger.error(f"Error generating geohash for lat={lat}, lon={lon}: {e}")
        return None

def load_restaurant_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load restaurant data from a CSV file and handle null values for latitude and longitude
    """
    restaurant_df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Filter and correct null latitude and longitude
    udf_fetch_geocoding = udf(lambda address: fetch_geocoding(address), StringType())
    restaurant_df = restaurant_df.withColumn(
        "latitude",
        col("latitude").cast(DoubleType())
    ).withColumn(
        "longitude",
        col("longitude").cast(DoubleType())
    ).withColumn(
        "geohash",
        udf(lambda lat, lon: generate_geohash(lat, lon), StringType())(col("latitude"), col("longitude"))
    )

    return restaurant_df

def load_weather_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load weather data from a CSV file
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)

def enrich_data(restaurant_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """
    Enrich restaurant data with weather data by performing a left join on geohash
    """
    enriched_df = restaurant_df.join(weather_df, "geohash", "left_outer")
    return enriched_df

def save_enriched_data(df: DataFrame, output_path: str):
    """
    Save the enriched DataFrame in Parquet format
    """
    df.write.mode("overwrite").partitionBy("geohash").parquet(output_path)
    logger.info(f"Enriched data saved to {output_path}")

def main():
    """
    Main ETL process
    """
    spark = create_spark_session()

    try:
        restaurant_data_path = "data/restaurant_data.csv"
        weather_data_path = "data/weather_data.csv"
        output_path = "output/enriched_data"

        # Check if input files exist
        if not os.path.exists(restaurant_data_path):
            logger.error(f"Restaurant data file not found: {restaurant_data_path}")
            raise FileNotFoundError(f"File not found: {restaurant_data_path}")

        if not os.path.exists(weather_data_path):
            logger.error(f"Weather data file not found: {weather_data_path}")
            raise FileNotFoundError(f"File not found: {weather_data_path}")

        # Load datasets
        restaurant_df = load_restaurant_data(spark, restaurant_data_path)
        weather_df = load_weather_data(spark, weather_data_path)

        # Enrich data
        enriched_df = enrich_data(restaurant_df, weather_df)

        # Save enriched data
        save_enriched_data(enriched_df, output_path)

        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(f"ETL process failed: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
