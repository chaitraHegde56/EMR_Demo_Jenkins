def read_file(spark_session):
    # df = spark_session.read.csv(str(config_dict['input-path']['dir']))
    data = ([1, 'a', 1000], [2, 'b', 2000], [3, 'c', 3000])
    schema = ['id', 'name', 'salary']
    df = spark_session.createDataFrame(data, schema)
    return df
