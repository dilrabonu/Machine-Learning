import unittest
import os
from bitcoin_price_converter import BitcoinPriceConverter

class TestBitcoinPriceConverter(unittest.TestCase):
    """Test cases for BitcoinPriceConverter class."""

    def setUp(self):
        """Set up test environment."""
        self.csv_url = "https://www.kaggle.com/datasets/team-ai/bitcoin-price-prediction?select=bitcoin_price_Training+-+Training.csv"
        self.converter = BitcoinPriceConverter(self.csv_url)

    def test_schema_definition(self):
        """Test if AVRO schema is correctly defined."""
        self.converter.define_avro_schema()
        self.assertIsNotNone(self.converter.avro_schema)

    def test_csv_loading(self):
        """Test if CSV data is loaded correctly."""
        self.converter.load_csv()
        self.assertIsNotNone(self.converter.df)
        self.assertTrue(len(self.converter.df) > 0)

    def test_avro_conversion(self):
        """Test if AVRO conversion works."""
        test_output = "test_output.avro"
        self.converter.load_csv()
        self.converter.define_avro_schema()
        self.converter.convert_to_avro(test_output)
        
        # Check if file exists and has size greater than 0
        self.assertTrue(os.path.exists(test_output))
        self.assertTrue(os.path.getsize(test_output) > 0)
        
        # Clean up
        os.remove(test_output)

if __name__ == '__main__':
    unittest.main()
