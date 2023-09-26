from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

SPARK_OUTPUT_PATH = '/home/airflow/airflow/Spark_Output'

spark = SparkSession\
    .builder\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0')\
    .master('local')\
    .appName('ass2')\
    .config('spark.mongodb.read.connection.uri', 'mongodb://127.0.0.1/airflow')\
    .getOrCreate()

# read questions
dfQ = spark.read \
    .format('mongodb') \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/airflow.questions")\
    .option('inferSchema', 'true') \
    .load()
# dfQ.printSchema()
dfQ = dfQ.withColumn('OwnerUserId', when(col('OwnerUserId') == 'NA', None).otherwise(col('OwnerUserId')))\
    .drop('_id')
dfQ = dfQ.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType()))\
    .withColumn('CreationDate', col('CreationDate').cast(DateType()))\
    .withColumn('ClosedDate', col('ClosedDate').cast(DateType()))
dfQ = dfQ.select('Id')

# read answers
dfA = spark.read \
    .format('mongodb') \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/airflow.answers")\
    .option('inferSchema', 'true') \
    .load()
# dfQ.printSchema()
dfA = dfA.withColumn('OwnerUserId', when(col('OwnerUserId') == 'NA', None).otherwise(col('OwnerUserId')))\
    .drop('_id')
dfA = dfA.withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType()))\
    .withColumn('CreationDate', col('CreationDate').cast(DateType()))
dfA = dfA.select('ParentId')
# join
join_expr = dfQ.Id == dfA.ParentId
join_df = dfQ.join(dfA, join_expr, 'left_outer')\
             .groupBy('Id')\
             .agg(count('*').alias('Number of answers'))
result_df = join_df.sort('Id')
result_df = result_df.repartition(1)
# write
result_df.write\
    .format('csv')\
    .mode('overwrite')\
    .option('path', SPARK_OUTPUT_PATH)\
    .option('header', 'true')\
    .save()
