"""ETL_fun.py 

This file contains the classes Extract, Load, Transform,
that hold the functionality of the ETL pipeline
Specifically:
     * class Extract
     
     * class Transform

     * class Load

"""
import os
import ast
import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *


class Extract(object):
    """The Extract class contains the methods that 
    correspond to the extraction step of the ETL pipeline.

    Attributes:
        None
    Methods:
        * extractDataJson()

        * extractDataDB()

        * preprocessTextFiles()
    """
    @staticmethod
    def extractDataJson(spark, path:str):
        """Loads Data from json formated files.

        Args:
            spark (sparkSession object): a sparkSession object.
            path (str): a path to the logs directory in the form path/to/dir.
        """
        dataset_logs = spark.read \
            .format('org.apache.spark.sql.json').load(path) \
            .select("date", "priceLow", "priceHigh", "areaIDs", "category",
             "listingType", "livingAreaLow", "livingAreaHigh", "newDevelopment", 
             "garage", "balcony", "secureDoor", "alarm", "fireplace", "elevator", 
             "garden", "roomsLow", "roomsHigh", "petsAllowed", "brokerID", "brokerIDs")
        dataset_logs.printSchema()

        return dataset_logs

    @staticmethod
    def extractDataJsonBuiltin(spark, path:str):
        jsonFile = pd.read_json("../../raw_data/json_dirs/2021-04-20-12/test", lines=True)
        return jsonFile

    @staticmethod
    def extractDataDB(spark, dbtable:str, path:str, driver="org.sqlite.JDBC"):
        """ Loads data from a database.

        Args:
            spark (sparkSession object): a sparkSession object.
            driver (str): component to interact with the database.
            dbtable (str): a schema.table to load.
            path (str): a path to the db (in our case contained in the local file system).

        Returns: 
            geography_db (pyspark.sql.dataframe.DataFrame): the loaded dataframe
        """
        url = "jdbc:sqlite:" + path
        geography_db = spark \
                    .read.format("jdbc") \
                    .option("url",url) \
                    .option("driver",driver) \
                    .option("dbtable",dbtable) \
                    .load()
        geography_db.printSchema()
        return geography_db

    @staticmethod
    def preprocessTextFiles(spark, path:str, fileName:str) -> None:
        """Transformed the logs to json friendly format. It makes use of pyspark RDDs.

        Args:
            spark (sparkSession object): a sparkSession object.
            path (str): a path to the logs directory in the form path/to/dir/.
            fileName (str): log name. 
        Returns:
            None
        """
        sc = spark.sparkContext                                                         # get the avaliable spark context
        distFile = sc.textFile(path + fileName)
        distFile_transformed = distFile.map(lambda x: json.dumps(ast.literal_eval(x)))
        distFile_transformed.coalesce(1).saveAsTextFile(path + fileName + "_tranformed")


class Transform(object):
    """ The Transform class contains methods for transforming 
    the given data.

    Attributes:
        None
    Methods:
        * clean_records()
        * replaceValues()
        * enrichData()
        * tranformDB()
    """

    @staticmethod
    def cleanRecords(data):
        """ Remove duplicates and records that contain only null values.
            In addition, the dataset is filtered and records made by brokers 
            or broker groups are removed.
        Args:
            data (spark.DataFrame): a DataFrame to be transformed.

        Returns:
            cleaned_data (spark.DataFrame): the processed DataFrame.
        """
        # drop duplicates
        dataTmp = data.dropDuplicates()
        # drop rows with null
        cleanedData = dataTmp.na.drop("all")
        # Remove the records where brokerID is not null
        dataFiltered_tmp = cleanedData.filter(cleanedData.brokerID.isNull())
        # filter the dataset and keep the records without brokerIDs
        dataFiltered = dataFiltered_tmp.withColumn("size", F.size("brokerIDs")). \
                    filter(F.col("size") <= 0).drop("size", "brokerID", "brokerIDs")
        
        return dataFiltered

    @staticmethod
    def replaceValues(data):
        """ Replace the null values for price and living area ranges according
            LivingAreaLow: 3
            LivingAreaHigh: 99999999
            SalePriceLow: 998
            SalePriceHigh: 99999999
            RentPriceLow: 9
            RentPriceHigh: 999999

        Args:
            data (spark.DataFrame): a DataFrame to be transformed.        
        Returns:
            transformedData (spark.DataFrame): tranformed data.
        """
        df = data.na.fill(3, "livingAreaLow")
        df2 = df.na.fill(99999999, "livingAreaHigh")

        transformedData1 = df2.withColumn("priceLow", \
                    F.when((df2.listingType=='sale') & (df2.priceLow.isNull()), 998). \
                    when((df2.listingType=='rent') & (df2.priceLow.isNull()), 9). \
                    otherwise(df2.priceLow))

        transformedData = transformedData1.withColumn("priceHigh", \
                    F.when((transformedData1.listingType=='sale') & (transformedData1.priceHigh.isNull()), 99999999). \
                    when((transformedData1.listingType=='rent') & (transformedData1.priceHigh.isNull()), 999999). \
                    otherwise(transformedData1.priceHigh))

        return transformedData
    
    @staticmethod
    def transformDB(spark, DB):
        """ Data that correspond to areaIDs are extracted from the geography DB
            to create a new DataFrame with the 3 following columns:
            |geographyID|parentId|country|

            Args:
                DB (spark.DataFrame): the geography database.
            Returns:
                newDB (spark.DataFrame): a new database.

            Note: this code used pandas. Hence it is not scalable.
                An other approach could be the use of a recursive CTE
                or graphframes for efficiency.

        """
        DB_new = DB.select("name", "geographyId", "parentId")        
        pandasDB = DB_new.toPandas()
        
        pandasDB["country"] = pandasDB["name"]                                     # create a new column
        pandasDB["geographyId"]  = pd.to_numeric(pandasDB["geographyId"])          # set the type to numeric
        pandasDB["parentId"] = pd.to_numeric(pandasDB["parentId"])

        pandasDB_reindexed = pandasDB.set_index('geographyId')                     # reindex the dataframe, using the geographyId as index
                                                                                   # reindexing is necessary in oder to fetch the values more
                                                                                   # efficiently.
        for index, row in pandasDB_reindexed.iterrows():
            # parentId  = 0, thus
            if row["parentId"] == 0:
                pandasDB_reindexed.at[index, "country"] = row["name"]
            else:
                # we need to look for the 'ancestors'
                parentId_tmp = row["parentId"]
                while parentId_tmp != 0:
                    curr_id_prev = parentId_tmp
                    parentId_tmp = pandasDB_reindexed.at[parentId_tmp, "parentId"]
                pandasDB_reindexed.at[index, "country"] = pandasDB_reindexed.at[curr_id_prev,"name"]
        
        # drop the unnecessary column name
        pandasDB_reindexed.drop(columns=["name"], inplace=True)
        # reset the index
        pandasDB_reindexed.reset_index(inplace=True)
        newDB = spark.createDataFrame(pandasDB_reindexed)
        return newDB


    @staticmethod
    def enrichData(data, DB):
        """ Enrich the dataset with additional information about geography.

        Args:
            data (spark.DataFrame): a DataFrame to be transformed.
            DB (spark.DataFrame): a spark DataFrame with extra information.

        Returns:
            enrichedData (spark.DataFrame): an enriched dataframe
        """
        # first the explode function is used to create a row for every areaID entry
        columns = data.schema.names
        columns = list(filter((lambda col: col != "areaIDs"), columns))
        data_exploded = data.select(*columns, F.explode(data.areaIDs).alias("areaID"))

        
        enriched_data = data_exploded.join(DB, data_exploded.areaID == DB.geographyId, 'left') \
                        .select(*columns, "areaID", "country")
        return enriched_data

class Load(object):
    """ The Load class contains methods that correspond to the 
        Load stage of the pipeline.

    Attributes:
        None
    Methods:
        * loadCleaned()
        * loadEnriched() 
    """
    @staticmethod
    def loadCleaned(pathOut: str, cleanedData) -> bool:
        """Write the cleaned data to Parquet file
            
        Args:
            pathOut (str): path to the outPut files in the form path/to/dir/.
            cleanedData (spark.DataFrame): the cleaned DataFrame to be stored.
        Returns:
            status (int): True if the write is successful False otherwise.

        """
        pathToCleanedData = pathOut + "clean_data/"
        try:
            cleanedData.write.parquet(pathToCleanedData, mode="overwrite")
            return True
        except:
            status = False

    @staticmethod
    def loadEnriched(pathOut: str, enrichedData) -> bool:
        """Write the enriched data to Parquet file
            
        Args:
            pathOut (str): path to the outPut files in the form path/to/dir/.
            enrichedData (spark.DataFrame): the cleaned DataFrame to be stored.
        Returns:
            status (int): True if the write is successful False otherwise.

        """
        pathToenrichedData = pathOut + "enriched/"

        try:
            # we choose to partition the data on country since the analysts perform analysis
            # on country level.
            enrichedData.write.parquet(pathToenrichedData, mode="overwrite", partitionBy=["country"])
            return True
        except:
            return False

if __name__== '__main__':
    pass