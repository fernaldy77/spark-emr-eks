### Import all the necessary Modules
import os
import sys
import os, sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import logging
import logging.config

from datetime import datetime
from subprocess import Popen, PIPE
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from util.create_objects import get_spark_object
import util.get_all_variables as gav

def load_files(spark, file_dir):
    """
    Load data
    :param spark: spark session
    :param file_dir: path of file to be read
    :return: dataframe of file
    """
    try:
        logging.info("load_files() is Started ...")
        df = spark. \
            read. \
            format('parquet'). \
            load({file_dir})

    except Exception as exp:
        logging.info("Error in the method - load_files(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info(f"The input File {file_dir} is loaded to the data frame. The load_files() Function is completed.")
    return df

def persist_data_s3(df, path):
    """
    Persist data to S3 bucket
    :param df: data to be stored
    :param path: S3 path
    :return:
    """
    try:
        df.write.mode("overwrite").parquet(path)
    except Exception as exp:
        logging.info("Error in the method - persist_data_s3(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info(f"The data is saved. The persist_data_s3() Function is completed.")
    return df

def main(month_partition, s3_bucket):
    """
    Main process
    :param month_partition: month partition of data
    :param s3_bucket: bucket ulr to be used
    :return:
    """
    try:
        logging.info("main() is started ...")

        # Get Spark Object
        spark = get_spark_object("TLC Analytics - Spark EMR on EKS")

        file_dir = f'{s3_bucket}/data/fhvhv_tripdata_{month_partition}.parquet'

        # Load data
        df = load_files(spark=spark, file_dir=file_dir)

        logging.info('Raw data:')
        logging.info(df.show())

        # Transform and summary data
        df = df.withColumn('date_partition', f.to_date(col('dropOff_datetime'), 'yyyy-MM-dd H:m:s'))
        df = df.withColumn('dispatching_base_num', col('dispatching_base_num').cast(StringType())) \
            .withColumn('PUlocationID', col('PUlocationID').cast(StringType())) \
            .withColumn('DOlocationID', col('DOlocationID').cast(StringType()))

        summary_df = df \
            .groupBy("date_partition", "dispatching_base_num", "PUlocationID", "DOlocationID") \
            .agg(f.count("dispatching_base_num").alias("count_trip"))
        summary_df = summary_df.withColumn('count_trip', col('count_trip').cast(IntegerType()))

        logging.info('Transformed data:')
        logging.info(summary_df.printSchema())
        logging.info(summary_df.show())

        # Persist data to s3
        path = f'{s3_bucket}/output/parquet/fhvhv_tripdata_summary/{month_partition}/'
        persist_data_s3(df=summary_df, path=path)

        logging.info(f"agg_fhvhv_tripcount.py is Completed.")

    except Exception as exp:
        logging.error("Error Occured in the main() method. Please check the Stack Trace to go to the respective module "
              "and fix it." +str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__" :
    logging.info("agg_fhvhv_tripcount is Started ...")

    # Take parameters
    month_partition = sys.argv[1]
    s3_bucket = sys.argv[1]

    main(month_partition, s3_bucket)