from pyspark.sql import SparkSession


MASTER_NODE_INSTANCE_NAME="[packt-dataproc-cluster-m]"

spark = SparkSession.builder \
    .appName('spark_hdfs_to_hdfs') \
    .getOrCreate()

sc = spak.sparkContext
sc.setLogLevel("WARN")

log_files_rdd = sc.textFile(f'hdfs://{MASTER_NODE_INSTANCE_NAME}/data/logs_example/*')
splitted_rdd = log_files_rdd.map(lambda x: x.split(" "))
selected_col_rdd = splitted_rdd.map(lambda x: (x[0], x[3], x[5], x[6]))

columns = ["ip","date","method","url"]
logs_df = selected_col_rdd.toDF(columns)
logs_df.createOrReplaceTempView('logs_df')