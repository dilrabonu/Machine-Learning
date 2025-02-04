import os
import sys
import platform
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

def check_python_compatibility():
    """Check Python version compatibility"""
    python_version = sys.version_info
    recommended_versions = [(3, 8), (3, 9), (3, 10)]
    
    logger.info(f"Current Python Version: {sys.version}")
    
    is_compatible = any(
        python_version.major == ver[0] and python_version.minor == ver[1] 
        for ver in recommended_versions
    )
    
    if not is_compatible:
        logger.warning(
            "Recommended Python versions for PySpark: 3.8, 3.9, 3.10. "
            "Current version may cause compatibility issues."
        )
    return is_compatible

def install_dependencies():
    """Install required dependencies"""
    dependencies = [
        "pyspark==3.3.2",
        "py4j==0.10.9.5",
        "typing_extensions",
        "pygeohash",
        "pytest",
        "python-dotenv"
    ]
    
    for dep in dependencies:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
            logger.info(f"Successfully installed {dep}")
        except subprocess.CalledProcessError:
            logger.error(f"Failed to install {dep}")

def configure_spark_environment():
    """Configure Spark environment variables"""
    env_vars = {
        "HADOOP_HOME": r"C:\hadoop",
        "SPARK_HOME": r"C:\spark",
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
        logger.info(f"Set {key} to {value}")

def verify_spark_installation():
    """Verify PySpark installation"""
    try:
        import pyspark
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("SparkSetupVerification") \
            .getOrCreate()
        
        logger.info(f"PySpark Version: {pyspark.__version__}")
        logger.info("Spark session created successfully")
        
        spark.stop()
        return True
    except ImportError:
        logger.error("PySpark not installed")
        return False
    except Exception as e:
        logger.error(f"Spark verification failed: {e}")
        return False

def main():
    """Main setup and verification script"""
    logger.info("Starting Spark Environment Setup")
    
    # Check Python compatibility
    if not check_python_compatibility():
        logger.warning("Python version may cause compatibility issues")
    
    # Install dependencies
    install_dependencies()
    
    # Configure environment
    configure_spark_environment()
    
    # Verify Spark installation
    if verify_spark_installation():
        logger.info("Spark environment setup completed successfully")
    else:
        logger.error("Spark environment setup failed")

if __name__ == "__main__":
    main()