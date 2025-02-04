from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def save_as_partitioned_parquet(df, output_path, partition_columns=None):
    """
    Save DataFrame as partitioned Parquet files
    
    Args:
        df (DataFrame): Input Spark DataFrame
        output_path (str): Path to save Parquet files
        partition_columns (list, optional): Columns to partition by
    """
    try:
        # Determine partitioning
        if partition_columns:
            df.write \
                .mode("overwrite") \
                .partitionBy(partition_columns) \
                .parquet(output_path)
        else:
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
        
        print(f"Data successfully saved to {output_path}")
    except Exception as e:
        print(f"Error saving data: {e}")

def main():
    spark = SparkSession.builder \
        .appName("DataSaving") \
        .getOrCreate()
    
    # Load joined data
    joined_df = spark.read.csv(
        'c:/Users/user/dilrabo/master-data/hotle/restaurant_csv', 
        header=True
    )
    
    # Output path
    output_path = 'c:/Users/user/dilrabo/master-data/hotle/enriched_data'
    
    # Save as partitioned Parquet
    save_as_partitioned_parquet(
        joined_df, 
        output_path, 
        partition_columns=['geohash']
    )
    
    spark.stop()

if __name__ == "__main__":
    main()