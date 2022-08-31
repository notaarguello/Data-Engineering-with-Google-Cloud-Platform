from pyspark.sql import SparkSession


BUCKET_NAME="packt-gcp-data-eng-notaarg-data-bucket"

spark = SparkSession.builder \
    .appName('spark_gcs_to_gcs') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

log_files_rdd = sc.textFile(f'gs://{BUCKET_NAME}/from-git/chapter-5/dataset/logs_example/*')
splitted_rdd = log_files_rdd.map(lambda x: x.split(" "))
selected_col_rdd = splitted_rdd.map(lambda x: (x[3], x[5], x[6]))

columns = ["date","method","url"]
logs_df = selected_col_rdd.toDF(columns)
logs_df.createOrReplaceTempView('logs_df')

sql = """
  SELECT
  url,
  count(*) as count
  FROM logs_df
  WHERE url LIKE '%/article%'
  GROUP BY url
  """
article_count_df = spark.sql(sql)
print(" ### Get only articles and blogs records ### ")
article_count_df.show(5)

# write back to gcs
#article_count_df.write.save(f'gs://{BUCKET_NAME}/from-git/chapter-5/job-result/article_count_df', format='csv', mode='overwrite')

# write to BQ table
article_count_df.write.format('bigquery') \
    .option('temporaryGcsBucket', BUCKET_NAME) \
    .option('table', 'dwh_bikesharing.article_count_df') \
    .mode('overwrite') \
    .save()
