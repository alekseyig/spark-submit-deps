"""Add PySpark to the library path based on the value of SPARK_HOME if
pyspark is not already in our path"""
try:
    from pyspark import context
except ImportError:
    import findspark
    findspark.init()
