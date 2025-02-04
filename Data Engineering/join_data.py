from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_weather_data(spark, weather_path):
    """
    Load weather data from specified path
    
    Args:
        spark (SparkSession): Active Spark session
        weather_path (str): Path to weather data
    
    Returns:
        DataFrame: Loaded weather DataFrame
    """
    return spark.read.parquet(weather_path)

def join_restaurant_weather(restaurant_df, weather_df):
    """
    Left join restaurant and weather data on geohash
    
    Args:
        restaurant_df (DataFrame): Restaurant DataFrame
        weather_df (DataFrame): Weather DataFrame
    
    Returns:
        DataFrame: Joined DataFrame
    """
    # Perform left join on geohash
    joined_df = restaurant_df.join(
        weather_df, 
        restaurant_df.geohash == weather_df.geohash, 
        "left"
    )
    
    # Select and rename columns to avoid duplicates
    final_df = joined_df.select(
        restaurant_df["*"],
        weather_df["temperature"],
        weather_df["precipitation"],
        weather_df["wind_speed"]
    )
    
    return final_df

def main():
    spark = SparkSession.builder \
        .appName("RestaurantWeatherJoin") \
        .getOrCreate()
    
    # Paths for restaurant and weather data
    restaurant_path = 'c:/Users/user/dilrabo/master-data/hotle/restaurant_csv'
    weather_path = 'c:/Users/user/dilrabo/master-data/hotle/weather'
    
    # Load data
    restaurant_df = spark.read.csv(restaurant_path, header=True)
    weather_df = load_weather_data(spark, weather_path)
    
    # Join data
    joined_df = join_restaurant_weather(restaurant_df, weather_df)
    
    # Show results
    joined_df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()