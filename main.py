from datetime import datetime, time
from datetime import date
from PowerService.PowerService import PowerService
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from sys import argv
from pyspark.sql.functions import udf
import pytz


from pyspark.sql.types import StructType, StructField, IntegerType, DateType, ArrayType,StringType
from pyspark.sql.functions import col, struct, when

from delta import *

def getFileName():
    filename = "PowerPosition_" + datetime.now(pytz.timezone("Europe/London") ).strftime("%Y%m%d_%H%M")
    return filename

def getFilePathAndName(path):
    return path + "/" + getFileName()+ ".csv"

def convertToTime(period):
    if period == 1 :
        return time(23,0).strftime("%H:00")
    else:
        return time(period -2 ,0).strftime("%H:00")

def getSparkSession() :
    session = SparkSession.builder \
        .appName("quickstart") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return session

def getPowerTradesSchema():
    return StructType([
        StructField('day', DateType(), False),
        StructField('volumeByPeriods', ArrayType(
            StructType([
                StructField('period', IntegerType(), True),
                StructField('volume', IntegerType(), True)])
        ), True)])


if __name__ == '__main__':

    session = getSparkSession()

    log4jLogger = session._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger("PowerPositions")

    #session.sparkContext.setLogLevel('INFO')

    args = argv[1:]
    finalDest = args[0]
    convertToTime_udf = udf(convertToTime, StringType())

    LOGGER.info("Intailizing PowerService API")

    powerservice = PowerService()
    LOGGER.info("Began to get data from Power service")
    powerTrades = powerservice.getTrades(date.today())
    LOGGER.info("End of getting data from Power Service and got {} sets of data".format(len(powerTrades)))
    
    powerTradesSchema = getPowerTradesSchema()

    if len(powerTrades) >0 : 

        LOGGER.info("Creating a data frame for powerTrades")
        powerTradesDf = session.createDataFrame(powerTrades, powerTradesSchema)

        LOGGER.info("expanding volumeByperiods List")
        explodeDf = powerTradesDf.select(powerTradesDf.day, explode(powerTradesDf.volumeByPeriods))

        LOGGER.info("Extracting volume and period from  volumeByperiods object")
        extractDf = explodeDf.select(explodeDf.day, col("col.period"), col("col.volume"))
        
        LOGGER.info("Writing data to delta table")
        try:
            extractDf.write.mode("append").partitionBy("day").format("delta").saveAsTable("powerTradesData_delta")
            

            LOGGER.info("Apply transformations and get calculate aggregrations")
            selectDf =  session.sql("SELECT period, sum(volume) as Volume FROM powerTradesData_delta where day ='" + date.today().strftime("%Y-%m-%d") + "' group by period order by period" )

            LOGGER.info("Coverting period to HH:MM format")
            resultDf = selectDf.withColumn("LocalTime", convertToTime_udf(selectDf["period"]))

            #resultDf.show()
            filePathName = getFilePathAndName(finalDest)
            LOGGER.info("Creating csv file in the following path {}".format(filePathName))
            resultDf.repartition(1).write.format("csv").option("header", True).save(getFilePathAndName(filePathName))

            LOGGER.info("Job Done. Congratutaltions")
        except:
            LOGGER.error("Error writing data to delta table")
        else:
            print("Content written successfully")
        
    

# See PyCharm help at https://www.jetbrains.com/help/pycharm/