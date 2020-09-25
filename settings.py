from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

conf = SparkConf().setMaster(os.getenv('SPARK_MASTER', 'local[*]'))\
    .setAppName("inverted_index_generator")\
    .setSparkHome(os.environ['SPARK_HOME'])
spark_context = SparkContext(conf=conf)
spark_session = SparkSession(spark_context)

dataset_location = "/Users/ShankarNag/Downloads/de-challenge-1/dataset"
output_location = os.path.join(dataset_location, 'id_files')
