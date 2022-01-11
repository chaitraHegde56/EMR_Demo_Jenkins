# This is a sample Python script.
import yaml
from pyspark.sql import SparkSession
from indices.indices import extract
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')


def get_config_dict(config_file):
    with open(config_file, 'r') as cfile:
        cfg = yaml.safe_load(cfile)
    return cfg


def etl():
    config_dict = dict()
    config_dict.update(get_config_dict('./config.yaml'))
    print("config_dict", config_dict)
    spark_session = SparkSession.builder.getOrCreate()
    #spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config_dict['s3']['access_key'])
    #spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config_dict['s3']['secret_key'])

    extract(spark_session, config_dict)


if __name__ == '__main__':
    etl()
