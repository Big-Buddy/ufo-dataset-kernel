from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit

spark = SparkSession.builder.master("local").appName("lab1").getOrCreate()
