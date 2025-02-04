import os
import sys

def configure_spark_environment():
    """
    Configures Spark environment variables and paths
    """
    # Try to find Spark installation
    spark_home_candidates = [
        r'C:\spark',  # Default Spark installation path
        r'C:\Program Files\Apache\spark',
        os.path.expanduser('~/spark'),
        os.environ.get('SPARK_HOME', '')
    ]

    spark_home = None
    for candidate in spark_home_candidates:
        if os.path.exists(candidate) and os.path.isdir(candidate):
            spark_home = candidate
            break

    if not spark_home:
        raise EnvironmentError(
            "Could not find Spark installation. "
            "Please install Spark or set SPARK_HOME environment variable."
        )

    # Set environment variables
    os.environ['SPARK_HOME'] = spark_home
    
    # Add Spark Python path
    spark_python_path = os.path.join(spark_home, 'python')
    sys.path.append(spark_python_path)

    # Add py4j to Python path
    py4j_path = os.path.join(spark_python_path, 'lib')
    py4j_zip = [f for f in os.listdir(py4j_path) if f.startswith('py4j-') and f.endswith('.zip')]
    
    if py4j_zip:
        sys.path.append(os.path.join(py4j_path, py4j_zip[0]))
    else:
        print("Warning: Could not find py4j zip file")

    # Import findspark to help locate Spark
    import findspark
    findspark.init(spark_home)

def main():
    configure_spark_environment()
    print("Spark environment configured successfully!")

if __name__ == "__main__":
    main()