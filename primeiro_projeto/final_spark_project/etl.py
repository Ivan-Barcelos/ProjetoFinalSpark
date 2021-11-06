# Import libraries
from pyspark.sql import SparkSession

# Create a Spark Session to connect to Spark
spark = SparkSession.builder.master("http://localhost:50070").appName("final_spark_project").getOrCreate()

print(spark.version, spark.sparkContext.master)

# Read a file from HDFS using spark connection
df_load = spark.read.csv("hdfs://namenode:8020/user/ivan/data/escola/alunos.csv")

# Show 5 lines of a file
df_load.show(5)

# Stop the Spark Session
spark.stop()