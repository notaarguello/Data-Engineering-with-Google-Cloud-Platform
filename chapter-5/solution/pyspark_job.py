from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('spark_hdfs_to_hdfs') \
    .getOrCreate()

sc = spak.sparkContext
sc.setLogLevel("WARN")