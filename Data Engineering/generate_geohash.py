# generate_geohash.py
import os
import sys
import typing

# Patch typing imports
class MockTypingIO:
    BinaryIO = typing.IO[bytes]

sys.modules['typing.io'] = MockTypingIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
import pygeohash as geohash
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_geohash(latitude, longitude, precision=4):
    """
    Generate a geohash for given latitude and longitude
    
    Args:
        latitude (float): Latitude coordinate
        longitude (float): Longitude coordinate
        precision (int): Length of geohash (default 4)
    
    Returns:
        str: Generated geohash or None if coordinates are invalid
    """
    try:
        if latitude is None or longitude is None:
            return None
        return geohash.encode(latitude, longitude, precision=precision)
    except Exception as e:
        logger.error(f"Error generating geohash: {e}")
        return None

def add_geohash_column(df):
    """
    Add geohash column to DataFrame
    
    Args:
        df (DataFrame): Input DataFrame with latitude and longitude
    
    Returns:
        DataFrame: DataFrame with added geohash column
    """
    # Create UDF for geohash generation
    geohash_udf = udf(generate_geohash, StringType())
    
    try:
        # Add geohash column
        df_with_geohash = df.withColumn(
            "geohash", 
            geohash_udf(df.latitude, df.longitude)
        )
        
        logger.info("Geohash column added successfully")
        return df_with_geohash
    except Exception as e:
        logger.error(f"Error adding geohash column: {e}")
        raise

def create_sample_dataframe(spark):
    """
    Create a sample DataFrame for testing
    
    Args:
        spark (SparkSession): Active Spark session
    
    Returns:
        DataFrame: Sample DataFrame with latitude and longitude
    """
    schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])
    
    data = [
        ("1", "Restaurant A", 40.7128, -74.0060),
        ("2", "Restaurant B", 34.0522, -118.2437),
        ("3", "Restaurant C", None, None)
    ]
    
    return spark.createDataFrame(data, schema)

def main():
    """
    Main function to generate geohashes
    """
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("GeohashGeneration") \
            .master("local[*]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        
        # Create sample restaurant DataFrame
        restaurant_df = create_sample_dataframe(spark)
        
        # Generate geohashes
        restaurant_df_with_geohash = add_geohash_column(restaurant_df)
        
        # Show sample results
        restaurant_df_with_geohash.show()
        
        # Stop Spark session
        spark.stop()
        
        logger.info("Geohash generation completed successfully")
    
    except Exception as e:
        logger.error(f"Geohash generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()