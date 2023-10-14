

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
// Import implicit encoders

import org.apache.spark.sql._

object Zip extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  // Create spark session
  val spark = SparkSession.builder().appName("Spark SQL basic example").enableHiveSupport().config("spark.master", "local").config("spark.sql.catalogImplementation", "hive").getOrCreate()
  val ds = spark.read.textFile("/Users/nkyokesh/Documents/EMR/test1.csv")
  ds.createOrReplaceGlobalTempView("zip")
  val ds_sql = spark.sql("select * from global_temp.zip")
  spark.table("global_temp.zip").show()
  spark.sql("show tables").show()
  ds_sql.show()


}
