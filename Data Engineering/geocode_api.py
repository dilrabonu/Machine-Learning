import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

class OpenCageGeocoder:
    """
    Geocoding service using OpenCage Geocoding API
    """
    def __init__(self, api_key=None):
        """
        Initialize OpenCage Geocoder
        
        Args:
            api_key (str, optional): OpenCage API Key. Defaults to environment variable.
        """
        self.api_key = api_key or os.environ.get('OPENCAGE_API_KEY')
        if not self.api_key:
            raise ValueError("OpenCage API Key is required")
    
    def geocode(self, restaurant_name):
        """
        Geocode a restaurant by its name
        
        Args:
            restaurant_name (str): Name of the restaurant
        
        Returns:
            tuple: (latitude, longitude) or (None, None)
        """
        base_url = "https://api.opencagedata.com/geocode/v1/json"
        params = {
            'q': restaurant_name,
            'key': self.api_key,
            'limit': 1
        }
        
        try:
            response = requests.get(base_url, params=params)
            data = response.json()
            
            if data['results']:
                location = data['results'][0]['geometry']
                return location['lat'], location['lng']
        except Exception as e:
            print(f"Geocoding error for {restaurant_name}: {e}")
        
        return None, None

def enrich_coordinates(spark_df, geocoder):
    """
    Enrich DataFrame with geocoded coordinates
    
    Args:
        spark_df (DataFrame): Input Spark DataFrame
        geocoder (OpenCageGeocoder): Geocoding service
    
    Returns:
        DataFrame: DataFrame with enriched coordinates
    """
    def geocode_restaurant(name):
        lat, lon = geocoder.geocode(name)
        return (lat, lon)
    
    # Create UDF for geocoding
    geocode_udf = udf(geocode_restaurant)
    
    # Enrich coordinates
    enriched_df = spark_df.withColumn(
        "geocoded_location", 
        geocode_udf(col("name"))
    )
    
    return enriched_df

def main():
    spark = SparkSession.builder \
        .appName("RestaurantGeocoding") \
        .getOrCreate()
    
    # Load restaurant data
    restaurant_df = spark.read.csv(
        'c:/Users/user/dilrabo/master-data/hotle/restaurant_csv', 
        header=True
    )
    
    # Initialize geocoder
    geocoder = OpenCageGeocoder()
    
    # Enrich coordinates
    enriched_df = enrich_coordinates(restaurant_df, geocoder)
    
    # Show results
    enriched_df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()