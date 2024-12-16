import pandas as pd
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import json
import requests
from typing import Dict, List
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BitcoinPriceConverter:
    """Class to handle conversion of Bitcoin price data from CSV to AVRO format."""
    
    def __init__(self, csv_path: str):
        """
        Initialize the converter with the CSV file path.
        
        Args:
            csv_path (str): Path to the CSV file containing Bitcoin price data
        """
        self.csv_path = csv_path
        self.df = None
        self.avro_schema = None

    def load_csv(self) -> None:
        """Load the CSV file into a pandas DataFrame."""
        try:
            # Create sample data if file doesn't exist
            if not os.path.exists(self.csv_path):
                logging.info("Creating sample Bitcoin price data...")
                self.create_sample_data()
            
            logging.info(f"Loading CSV data from {self.csv_path}...")
            self.df = pd.read_csv(self.csv_path)
            logging.info(f"Successfully loaded CSV with {len(self.df)} rows")
        except Exception as e:
            logging.error(f"Error loading CSV: {str(e)}")
            raise

    def create_sample_data(self) -> None:
        """Create sample Bitcoin price data."""
        sample_data = {
            'Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'Open': [42000.0, 42500.0, 43000.0, 43500.0, 44000.0],
            'High': [42800.0, 43300.0, 43800.0, 44300.0, 44800.0],
            'Low': [41500.0, 42000.0, 42500.0, 43000.0, 43500.0],
            'Close': [42500.0, 43000.0, 43500.0, 44000.0, 44500.0],
            'Volume': [1000000, 1100000, 1200000, 1300000, 1400000]
        }
        df = pd.DataFrame(sample_data)
        df.to_csv(self.csv_path, index=False)
        logging.info(f"Created sample data file: {self.csv_path}")

    def define_avro_schema(self) -> None:
        """Define the AVRO schema based on the CSV structure."""
        try:
            schema_dict = {
                "namespace": "bitcoin.price.history",
                "type": "record",
                "name": "BitcoinPrice",
                "fields": [
                    {"name": "Date", "type": "string"},
                    {"name": "Open", "type": "double"},
                    {"name": "High", "type": "double"},
                    {"name": "Low", "type": "double"},
                    {"name": "Close", "type": "double"},
                    {"name": "Volume", "type": "double"}
                ]
            }
            
            self.avro_schema = avro.schema.parse(json.dumps(schema_dict))
            logging.info("AVRO schema defined successfully")
        except Exception as e:
            logging.error(f"Error defining AVRO schema: {str(e)}")
            raise

    def convert_to_avro(self, output_file: str) -> None:
        """
        Convert the CSV data to AVRO format and save it.
        
        Args:
            output_file (str): Path to save the AVRO file
        """
        try:
            with DataFileWriter(open(output_file, "wb"), DatumWriter(), self.avro_schema) as writer:
                for _, row in self.df.iterrows():
                    record = {
                        'Date': str(row['Date']),
                        'Open': float(row['Open']),
                        'High': float(row['High']),
                        'Low': float(row['Low']),
                        'Close': float(row['Close']),
                        'Volume': float(row['Volume'])
                    }
                    writer.append(record)
            logging.info(f"Successfully converted data to AVRO format: {output_file}")
        except Exception as e:
            logging.error(f"Error converting to AVRO: {str(e)}")
            raise

    def describe_avro_file(self, avro_file: str) -> None:
        """
        Describe the AVRO file format, including structure, schema, and data.
        
        Args:
            avro_file (str): Path to the AVRO file
        """
        try:
            logging.info("\nAVRO File Description:")
            logging.info("1. File Structure:")
            logging.info("   - Format: Apache AVRO binary format")
            logging.info("   - Compression: None")
            
            logging.info("\n2. Schema Definition:")
            logging.info(f"   {json.dumps(json.loads(str(self.avro_schema)), indent=4)}")
            
            logging.info("\n3. Sample Records:")
            reader = DataFileReader(open(avro_file, "rb"), DatumReader())
            records = []
            for record in reader:
                records.append(record)
                if len(records) >= 5:  # Print first 5 records
                    break
            for record in records:
                logging.info(f"   {record}")
            
            reader.close()
            
            logging.info("\n4. AVRO Benefits for Big Data:")
            logging.info("   a) Schema Evolution:")
            logging.info("      - Supports forward and backward compatibility")
            logging.info("      - Allows schema changes without breaking existing code")
            
            logging.info("   b) Binary Format:")
            logging.info("      - Compact storage compared to text formats like CSV")
            logging.info("      - Efficient serialization and deserialization")
            
            logging.info("   c) Rich Data Types:")
            logging.info("      - Supports complex data types (arrays, maps, nested records)")
            logging.info("      - Built-in type safety and validation")
            
            logging.info("   d) Language Agnostic:")
            logging.info("      - Can be used with multiple programming languages")
            logging.info("      - Ideal for polyglot environments")
            
            logging.info("   e) Integration:")
            logging.info("      - Native support in Hadoop ecosystem")
            logging.info("      - Excellent for data streaming and processing")
            
        except Exception as e:
            logging.error(f"Error describing AVRO file: {str(e)}")
            raise

    def print_sample_records(self, num_records: int = 5) -> None:
        """
        Print sample records from the DataFrame.
        
        Args:
            num_records (int): Number of records to print
        """
        logging.info(f"\nFirst {num_records} records from the dataset:")
        print(self.df.head(num_records))

def main():
    # File paths
    csv_path = "bitcoin_price_data.csv"
    avro_path = "bitcoin_price_history.avro"
    
    try:
        # Initialize converter
        converter = BitcoinPriceConverter(csv_path)
        
        # Load CSV data
        converter.load_csv()
        
        # Print sample records
        converter.print_sample_records()
        
        # Define AVRO schema
        converter.define_avro_schema()
        
        # Convert to AVRO
        converter.convert_to_avro(avro_path)
        
        # Describe the AVRO file
        converter.describe_avro_file(avro_path)
        
        logging.info("Conversion process completed successfully!")
        
    except Exception as e:
        logging.error(f"An error occurred during the conversion process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
