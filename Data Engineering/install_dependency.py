import sys
import subprocess
import platform
import os

def run_command(command, capture_output=False):
    """
    Run shell command with error handling
    """
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            capture_output=capture_output, 
            text=True
        )
        return result.stdout if capture_output else None
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"Error output: {e.stderr}")
        return None

def check_system_requirements():
    """
    Check system compatibility for PySpark installation
    """
    print("System Compatibility Check:")
    print(f"Python Version: {sys.version}")
    print(f"Platform: {platform.platform()}")
    print(f"Python Executable: {sys.executable}")

def install_java():
    """
    Check and install Java if not present
    """
    try:
        java_version = run_command("java -version", capture_output=True)
        if java_version:
            print("Java is already installed:")
            print(java_version)
            return True
    except:
        print("Java not found. Please install Java 8 or later.")
        print("Download from: https://www.java.com/en/download/")
        return False

def install_dependencies():
    """
    Install PySpark and related dependencies
    """
    dependencies = [
        "pip install --upgrade pip setuptools wheel",
        "pip install pyspark==3.3.2",
        "pip install py4j==0.10.9.5",
        "pip install typing_extensions",
        "pip install pygeohash",
        "pip install pytest",
        "pip install python-dotenv"
    ]

    for dep in dependencies:
        print(f"Running: {dep}")
        run_command(dep)

def verify_pyspark():
    """
    Verify PySpark installation
    """
    verification_commands = [
        "python -c \"import pyspark; print('PySpark Version:', pyspark.__version__)\"",
        "python -c \"from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); print('Spark Session Created');\""
    ]

    for cmd in verification_commands:
        print(f"Verifying: {cmd}")
        run_command(cmd)

def configure_environment():
    """
    Set environment variables for Spark
    """
    env_vars = {
        "HADOOP_HOME": r"C:\hadoop",
        "SPARK_HOME": r"C:\spark",
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable
    }

    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"Set {key} to {value}")

def main():
    """
    Main installation and verification script
    """
    print("PySpark Installation and Verification Script")
    print("=" * 50)

    # Check system requirements
    check_system_requirements()

    # Install Java (if not present)
    if not install_java():
        print("Java installation is required. Exiting.")
        sys.exit(1)

    # Install dependencies
    install_dependencies()

    # Configure environment
    configure_environment()

    # Verify PySpark installation
    verify_pyspark()

    print("\nInstallation process completed.")

if __name__ == "__main__":
    main()