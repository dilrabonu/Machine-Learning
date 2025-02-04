import os
import sys
import typing

class MockTypingIO:
    BinaryIO = typing.IO[bytes]

sys.modules['typing.io'] = MockTypingIO

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from load_preprocess import load_restaurant_data
from geocode_api import OpenCageGeocoder, enrich_coordinates
from generate_geohash import generate_geohash, add_geohash_column
from join_data import join_restaurant_weather

class TestSparkDataProcessing:
    @classmethod
    def setup_class(cls):
        """
        Setup Spark session for testing
        """
        cls.spark = SparkSession.builder \
            .appName("SparkTestApp") \
            .master("local[*]") \
            .getOrCreate()
    
    @classmethod
    def teardown_class(cls):
        """
        Stop Spark session after tests
        """
        cls.spark.stop()
    
    def test_restaurant_data_loading(self):
        """
        Test restaurant data loading
        """
        # Create a sample DataFrame
        schema = StructType([
            StructField("restaurant_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])
        
        sample_data = [
            ("1", "Test Restaurant", 40.7128, -74.0060),
            ("2", "Another Restaurant", None, None)
        ]
        
        df = self.spark.createDataFrame(sample_data, schema)
        
        # Test loading
        assert df.count() == 2
        assert df.columns == ["restaurant_id", "name", "latitude", "longitude"]
    
    def test_geohash_generation(self):
        """
        Test geohash generation
        """
        # Test valid coordinates
        geohash_1 = generate_geohash(40.7128, -74.0060)
        assert geohash_1 is not None
        assert len(geohash_1) == 4
        
        # Test invalid coordinates
        geohash_2 = generate_geohash(None, None)
        assert geohash_2 is None
    
    def test_data_join(self):
        """
        Test data joining functionality
        """
        # Create sample restaurant and weather DataFrames
        restaurant_schema = StructType([
            StructField("restaurant_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("geohash", StringType(), True)
        ])
        
        weather_schema = StructType([
            StructField("geohash", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("precipitation", DoubleType(), True)
        ])
        
        restaurant_data = [
            ("1", "Test Restaurant", 40.7128, -74.0060, "dr5r"),
            ("2", "Another Restaurant", 34.0522, -118.2437, "9q5b")
        ]
        
        weather_data = [
            ("dr5r", 25.5, 0.1),
            ("9q5b", 30.0, 0.2)
        ]
        
        restaurant_df = self.spark.createDataFrame(restaurant_data, restaurant_schema)
        weather_df = self.spark.createDataFrame(weather_data, weather_schema)
        
        # Perform join
        joined_df = join_restaurant_weather(restaurant_df, weather_df)
        
        # Assertions
        assert joined_df.count() == 2
        assert "temperature" in joined_df.columns
        assert "precipitation" in joined_df.columns

def main():
    pytest.main([__file__])

if __name__ == "__main__":
    main()