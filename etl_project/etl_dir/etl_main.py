"""etl_main.py

   The main script where the pipeline is defined.

   In the main() function the sparkSession is initialized and configured,
   along with the logging mechanism.

   For logging see: https://www.timmitchell.net/post/2016/03/14/etl-logging/

   The ETL (extract, tranform, load) pipeline follows through three different functions
   (located on helpers.py)

   Requires: findSpark, pandas, numpy, pyspark.
    
   Author: Nina Siaminou, June 2021
    
"""

import json
import sys
from os import path

import findspark
import numpy as np
import pandas as pd

from ETL_fun import Extract, Transform, Load


def initializeLogging(spark):
    """
    Function to declare a customized log4j instance.

    Args:
        spark (sparkSession object): the initialized sparkSession object.
    
    Returns:
        logger (log4j object): a customized apache.log4j logger.
    """
    sc = spark.sparkContext.getConf()
    appName = sc.get('spark.app.name')

    logger4j = spark._jvm.org.apache.log4j       
    logger = logger4j.LogManager.getLogger(f"APP_LOGGER: Application name: {appName} -> script name: {__name__}")
    return logger        

def main() -> None:
    """ 
    Main function that implements the ETL process through the necessary function calls.

    Returns:
        None
    """
    # First initialize the paths to look for the data sources
    pathDB = "../../raw_data/geography.sqlite"
    pathLogs = "../../raw_data/transformed_data"

    # path to the directory for the output files
    pathOut = "../../out_data/"

    # First, spark session is initialized.
    spark = SparkSession.builder. \
            master("local[*]"). \
            config('spark.jars.packages', 'sqlite-jdbc-3.34.0.jar'). \
            appName("ETL_pipeline"). \
            getOrCreate()
    spark.sparkContext.setLogLevel('WARN')                                                    # to suppress the info level logging
    # initialize logger
    logger = initializeLogging(spark)
    
    # Extract
    logger.warn("Extraction process")
    try:
        rawData = Extract.extractDataJson(spark, path=pathLogs)
        geographyDB = Extract.extractDataDB(spark, dbtable="geography", path=pathDB)
    except:
        logger.error("Extraction process Failed")
        spark.stop()
        exit()
    
    # Transform
    logger.warn("Transform process")
    denormalizedDB = Transform.transformDB(spark, geographyDB)
    cleanData = Transform.cleanRecords(rawData)
    transformedData = Transform.replaceValues(cleanData)
    enrichedData = Transform.enrichData(transformedData, denormalizedDB)

    # Load
    logger.warn("Load process") 
    status_clean = Load.loadCleaned(pathOut, transformedData)
    status_enriched = Load.loadEnriched(pathOut, enrichedData)
    if not (status_clean and status_enriched):
        logger.warn("Load process failed")
    else:
        logger.warn(f"Pipeline Completed data saved at {pathOut}")

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
