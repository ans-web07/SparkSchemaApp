from pyspark.sql import *
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaApp") \
        .getOrCreate()

    logger = Log4J(spark)

    # Reading from .csv file
    flightCsvDf= spark.read\
         .format("csv")\
         .option("header","true")\
         .option("inferschema","true")\
         .load("data/flight-time.csv")

    flightCsvDf.show(5)
    logger.info("CSV schema: "+ flightCsvDf.schema.simpleString())

    #defining Schema programmatically
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    #setting mode for reporting err and date format(needed otherwise parsing exception will occur)
    # below while reading data
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight-time.csv")

    flightCsvDf.show(5)
    logger.info("CSV schema defined programmatically result: " + flightCsvDf.schema.simpleString())

    #Reading from .json file
    flightJsonDf = spark.read \
        .format("json") \
        .load("data/flight-time.json")

    flightJsonDf.show(5)
    logger.info("JSON schema: " + flightJsonDf.schema.simpleString())

    #defining schema through DDL
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    #Reading data using flightSchemaDDl
    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load("data/flight-time.json")

    flightTimeJsonDF.show(5)
    logger.info("JSON Schema using Schema defined by DDL:" + flightTimeJsonDF.schema.simpleString())

    # Reading from .parquet file
    flightParquetDf = spark.read \
        .format("parquet") \
        .load("data/flight-time.parquet")

    flightParquetDf.show(5)
    logger.info("Parquet schema: " + flightParquetDf.schema.simpleString())