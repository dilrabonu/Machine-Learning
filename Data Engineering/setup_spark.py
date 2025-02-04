import os
import sys
import subprocess

def install_dependencies():
    """Install required dependencies"""
    dependencies = [
        'typing_extensions',
        'pyspark==3.3.2', 
        'py4j==0.10.9.5'
    ]
    
    for dep in dependencies:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', dep])
            print(f"Installed {dep}")
        except Exception as e:
            print(f"Error installing {dep}: {e}")
    
    # Ensure typing_extensions is imported
    import typing_extensions

def patch_typing_imports():
    """
    Patch typing imports to resolve compatibility issues
    """
    import sys
    import typing

    # Add a mock typing.io module
    class MockTypingIO:
        BinaryIO = typing.IO[bytes]

    sys.modules['typing.io'] = MockTypingIO

def setup_spark_environment():
    # Install dependencies first
    install_dependencies()
    
    # Patch typing imports
    patch_typing_imports()
    
    # Set Hadoop and Spark home directories
    hadoop_home = r'C:\hadoop'
    spark_home = r'C:\spark'
    
    # Set environment variables
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['SPARK_HOME'] = spark_home
    
    # Verify PySpark installation
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("SparkSetup") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        print("Spark is successfully configured!")
        spark.stop()
        return True
    except Exception as e:
        print(f"Error configuring Spark: {e}")
        return False

def main():
    if setup_spark_environment():
        print("Spark environment is ready to use.")
    else:
        print("Please resolve the above issues before proceeding.")

if __name__ == "__main__":
    main()