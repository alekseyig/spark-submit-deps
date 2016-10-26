import findspark

try:
    from pyspark import context
except ImportError:
    # Add PySpark to the library path based on the value of SPARK_HOME if
    # pyspark is not already in our path
    findspark.init()

findspark.add_packages(['com.databricks:spark-csv_2.10:1.4.0'])
