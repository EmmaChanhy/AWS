import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import date, timedelta, datetime

import os
import json
import pandas as pd
import boto3
import io
import time
import logging

from pyspark.sql import SparkSession,SQLContext,HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

os.environ['TZ'] = 'Hongkong'
time.tzset()
current_time = datetime.now()
cur_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
batch_year = current_time.strftime("%Y")
batch_month = current_time.strftime("%m")
batch_day = current_time.strftime("%d")

spark = SparkSession.builder.           \
  appName("Emma1.0").  \
  config("spark.databricks.hive.metastore.glueCatalog.enabled", "true"). \
  enableHiveSupport(). \
  getOrCreate()
sc= spark.sparkContext
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
logging.basicConfig(format=msg_format, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("[JOBINFO]")
logger.setLevel(logging.INFO)

# pySpark
df = spark.read.option("header","true").csv("s3_raw_data_path/file_name.csv")
df.printSchema()
df.show(10)
df.write.partitionBy("Date").format("parquet").save("s3_processed_data_path")

# sparkSQL
df = spark.read.option("header", "true").csv("s3_raw_data_path/file_name.csv")
df.createOrReplaceTempView("table")
df = spark.sql("SELECT * FROM table WHERE column_name='string' ")
df.show(10)