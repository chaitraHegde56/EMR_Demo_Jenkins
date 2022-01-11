# from ..common.utils import read_file
from common.utils import read_file


def extract(spark_session, config_dict):
    df = read_file(spark_session)
    df = transform(df)
    load(df, config_dict)


def transform(df):
    return df


def load(df, config_dict):
    df.show(4)
    df.write.csv('s3a://emr-demos/emr_test_output1/')
    # df.write.csv(config_dict['output-path']['dir'])
