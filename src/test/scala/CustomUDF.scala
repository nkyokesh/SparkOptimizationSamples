

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class CustomUDF extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)
  test("Should register custom UDF") {
    //given
    val dataset = spark
      .createDataFrame(Seq((0, "some text one"), (1, "some text two")))
      .toDF("id", "text")

    val upper: String => String = _.toUpperCase

    //when
    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)

    val res = dataset.withColumn("upper", upperUDF(dataset("text")))
    res.show()

  }

}
