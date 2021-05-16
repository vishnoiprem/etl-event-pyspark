import findspark

findspark.init()
import os
from pathlib import Path
from pyspark.sql.session import SparkSession
import logging
from datetime import datetime
from importlib_resources import files


class EventTransform(object):

    def print_df(self):
        print('rating')


# /* Reads file in the resource path and returns the content
#     *
#     * @param filename name of the file
#     * @return String content of resource path file
#     */
def loadResource(filename):
    current_path = Path(os.path.dirname(os.path.realpath(__file__)))
    resources="/resources"
    fname = "{}{}{}".format(current_path,resources ,filename)
    f_read=open(fname, 'r').read()
    return f_read


def loadCleansedData(spark, path):
    eventDF = spark.read.json(path)
    eventDF.createOrReplaceTempView("event")
    cleansedDF = spark.sql(loadResource("/sql/cleansing.sql"))
    cleansedDF.createOrReplaceTempView("event_master")



  #
  # /** Performs transformation at user level and saves the result as external hive table in CSV format
  #   *
  #   * @param spark Spark Session created
  #   * @param userTableLoc path of external hive table
  #   *
  #   */

def userTransform(spark, userTableLoc):

    usersDF = spark.sql(loadResource("/sql/user_transform.sql"))
    usersDF.show()
    usersDF.write.format("csv").mode('overwrite').option("nullValue", "NULL").save(userTableLoc)
    usersDF.createOrReplaceTempView("users")
    ct="""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_details 
        (
            user_id STRING
            ,time_stamp STRING
            ,url_level1 STRING
            ,url_level2 STRING
            ,url_level3 STRING
            ,activity STRING
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{}'
     """.format(userTableLoc)

    spark.sql(ct)
    spark.sql("SELECT * FROM user_details").show()


  # /** Performs transformation at hourly level and saves the result as external hive table in CSV format
  #   *
  #   * @param spark Spark Session created
  #   * @param activityTableLoc path of external hive table
  #   *
  #   */

def activityTransform(spark, activityTableLoc):
    activityDF=spark.sql(loadResource("/sql/activity_transform.sql"))
    activityDF.coalesce(1).write\
      .format("csv") \
       .mode('overwrite')\
      .option("nullValue", "NULL")\
      .save(activityTableLoc)

    ct="""
        CREATE EXTERNAL TABLE IF NOT EXISTS activity_details 
        (
            time_bucket STRING
            ,url_level1 STRING
            ,url_level2 STRING
            ,activity STRING
            ,activity_count STRING
            ,user_count STRING
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{}'
     """.format(activityTableLoc)
    spark.sql(ct)
    spark.sql("SELECT * FROM activity_details").show()
    #demo purpose

  # Performs frequently searched trade in a month and saves the result as external hive table in CSV format
  #
  # @param spark Spark Session created
  # @param tradeTableLoc path of external hive table
  #

def findPopularTrade(spark, tradeTableLoc):

    tradeDF=spark.sql(loadResource("/sql/trade_demand_transform.sql"))
    tradeDF.coalesce(1).write\
    .format("csv") \
    .mode('overwrite') \
    .option("nullValue", "NULL")\
    .save(tradeTableLoc)

    ct="""
        CREATE EXTERNAL TABLE IF NOT EXISTS trade_demand 
        (
            month_bucket STRING
            ,trade STRING
            ,search_frequency STRING
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{}'
     """.format(tradeTableLoc)

    spark.sql(ct)
    spark.sql("SELECT * FROM trade_demand").show()
    #demo purpose





if __name__ == "__main__":
    # Spark start point
    spark = SparkSession.builder \
        .appName("Spark-Hipages  SQL ETL App") \
        .config("spark.driver.host", "localhost") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = logging.getLogger('alexTest')
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s  %(msg)s : %(asctime)s  "))
    logger.addHandler(_h)
    logger.setLevel(logging.DEBUG)
    logger.info("module imported and logger initialized")

    # input and outout file varicable.

    args1 = "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_event_data.json"
    args2 = "/Users/vishnoiprem/tmp/etl/"

    logger.info("1.Lets make data clean")
    loadCleansedData(spark, args1)
    logger.info("1.Cleansed JSON load completed")

    logger.info("2.user_details transform started")
    userTableLoc = args2 + "/user_details/"
    userTransform(spark,userTableLoc)
    logger.info("2.user_details transform completed")


    logger.info("3.activity_details transform started")
    activityTableLoc = args2 + "/activity_details/"
    activityTransform(spark,activityTableLoc)
    logger.info("3.activity_details transform completed")

    logger.info("4.Frequently searched trade transform started")
    tradeTableLoc = args2 + "/trade_demand/"
    findPopularTrade(spark,tradeTableLoc)
    logger.info("4.Frequently searched trade transform completed")
    spark.stop()