from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


# Create a Spark session
spark = SparkSession.builder \
   .appName("UDF Example") \
   .getOrCreate()


# Sample data
data = [(1,), (2,), (3,), (4,)]
columns = ["number"]


# Create DataFrame
df = spark.createDataFrame(data, columns)


# Define custom function
def square(x):
   return x ** 2
