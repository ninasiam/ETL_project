"""preprocess_data.py 
   Script to tranform a log file to a json format.
   
"""
import json
import os
from posix import listdir
from typing import Counter

import findspark
import numpy as np
import pandas as pd

from ETL_fun import Extract
from etl_main import initializeLogging


def main():
    path = "../../raw_data/data/"
    fileName = "data20"

    # First, spark session is initialized.
    spark = SparkSession.builder. \
            master("local[*]"). \
            appName("Preprocess_data"). \
            getOrCreate()

    logger = initializeLogging(spark)
    logger.info(f"Tranform data file {fileName} to json format")

    Extract.preprocessTextFiles(spark, path, fileName)

    spark.stop()

if __name__ == "__main__":

    findspark.init()                                                                            # look for spark
    # In case pyspark is not available the program terminates.
    try:
        from pyspark.sql import SparkSession

        # call main function
        main()
    except ImportError as error:
        raise ImportError('Pyspark was not found!')
        exit()
