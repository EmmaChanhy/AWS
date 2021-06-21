%pip install awswrangler

%pip install xlrd

%pip install openpyxl

import sys
from datetime import date, timedelta, datetime

import os
import json
import pandas as pd
import boto3
import io
import time
import awswrangler as wr
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

print(batch_year)

s3r = boto3.resource('s3')
s3 = boto3.client('s3',region_name= 'ap-southeast-1')

bucket_name = 's3_bucket_name'
raw_prefix = 's3_raw_prefix'
archive_prefix = 's3_archive_prefix'

s3_res = s3.list_objects_v2(Bucket = bucket_name, Prefix = raw_prefix)
if 'Contents' in s3_res:
    s3_objects = s3_res['Contents']
    for s3_object in s3_objects: 
        key = s3_object['Key']
        if 'xlsx' in key:
            print(f'key: {key}')
            raw_data = pd.read_excel(f's3://{bucket_name}/{key}',engine='openpyxl',dtype='str', sheet_name=[0,1])
            unique_id = key.split('/')[4].split(' ')[1]
            print(f'unique_id: {unique_id}')
            file_year = key.split('/')[4].split('_')[1].split('.')[0][0:4]
            file_month = key.split('/')[4].split('_')[1].split('.')[0][4:6]
            file_day = key.split('/')[4].split('_')[1].split('.')[0][6:8]
            print(f'file_year: {file_year}')
            print(f'file_month: {file_month}')
            print(f'file_day: {file_day}')
            
            for i in range(2):
                n = raw_data[i].shape[1]
                print(f'shape: {n}')
                lst = ['']*n
                if raw_data[i].empty:
                    raw_data[i].loc[len(raw_data[i])] = lst
                
                raw_data[i]['file_year']=file_year
                raw_data[i]['file_month']=file_month
                raw_data[i]['file_day']=file_day
                raw_data[i]['unique_id']=unique_id
                display(raw_data[i])
                print(raw_data[i].dtypes)
                
                if i==0:
                    wr.s3.to_parquet(
                        df = raw_data[i],
                        path=f's3_output_path/{unique_id}',
                        dataset = True,
                        partition_cols=['file_year','file_month','file_day'])
                elif i==1:
                    wr.s3.to_parquet(
                        df = raw_data[i],
                        path=f's3_output_path/{unique_id}',
                        dataset = True,
                        partition_cols=['file_year','file_month','file_day'])
            
        elif 'csv' in key:
            print(f'key: {key}')
            unique_id = key.split('/')[4].split(' ')[1]
            print(f'unique_id: {unique_id}')
            file_year = key.split('/')[4].split('_')[1].split('.')[0][0:4]
            file_month = key.split('/')[4].split('_')[1].split('.')[0][4:6]
            file_day = key.split('/')[4].split('_')[1].split('.')[0][6:8]
            print(f'file_year: {file_year}')
            print(f'file_month: {file_month}')
            print(f'file_day: {file_day}')
            
            csv_data = pd.read_csv(f's3://{bucket_name}/{key}',dtype='str')
            csv_data['unique_id']=unique_id
            csv_data['file_year']=file_year
            csv_data['file_month']=file_month
            csv_data['file_day']=file_day
            print(csv_data.head())
            
            wr.s3.to_parquet(
                df = csv_data,
                path=f's3_output_path/{unique_id}',
                dataset = True,
                partition_cols=['file_year','file_month','file_day'])