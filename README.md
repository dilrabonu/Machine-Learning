# Bitcoin Price History Data Processing

This project converts Bitcoin price history data from CSV format to AVRO format, demonstrating the advantages of using AVRO for handling large datasets in Big Data environments.

## Features

- Converts CSV data to AVRO format with proper schema definition
- Includes error handling and logging
- Provides sample record printing functionality
- Includes unit tests for validation
- Well-documented code with type hints

## Requirements

- Python 3.7+
- Required packages listed in `requirements.txt`

## Installation

1. Clone this repository
2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the main script:
```bash
python bitcoin_price_converter.py
```

Run tests:
```bash
python -m unittest test_bitcoin_price_converter.py
```

## Project Structure

- `bitcoin_price_converter.py`: Main script for CSV to AVRO conversion
- `test_bitcoin_price_converter.py`: Unit tests
- `requirements.txt`: Project dependencies
- `README.md`: Project documentation

## AVRO Format Benefits

1. Schema Evolution: AVRO supports schema evolution, allowing for changes in data structure over time
2. Compact Serialization: More efficient storage compared to CSV
3. Rich Data Types: Supports complex data types and nested structures
4. Language Agnostic: Can be used with multiple programming languages
5. Built-in Documentation: Schema provides self-documenting data structure
