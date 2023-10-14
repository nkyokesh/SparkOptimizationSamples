
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MinimizeObjectCreation extends AnyFunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  spark.setLogLevel("WARN")
  test("should leverage aggregateByKey to reuse state object") {
    //given
    val keysWithValuesList =
      Array(
        MetricsData("127.0.0.1", 20L),
        MetricsData("127.0.0.2", 350L),
        MetricsData("127.0.0.1", 24L),
        MetricsData("127.0.0.2", 200L),
        MetricsData("191.0.0.1", 10L)
      )
    val data = spark.parallelize(keysWithValuesList)
    val keyed = data.keyBy(_.host)

    val responseTimesForHost = mutable.ArrayBuffer.empty[Long]
    val addResponseTime = (responseTimes: mutable.ArrayBuffer[Long], metricsData: MetricsData) => responseTimes += metricsData.responseTime
    val mergeResponseTimes = (p1: mutable.ArrayBuffer[Long], p2: mutable.ArrayBuffer[Long]) => p1 ++= p2
    // Here we use mutable ArrayBuffer to reuse the same buffer to add the responseTimes instead of creating new.
    // Host keys are paired with KeyBy as pairedRDD and responseTimes will be added in same ArrayBuffer

    //when
    val aggregatedResponseTimes = keyed
      .aggregateByKey(responseTimesForHost)(addResponseTime, mergeResponseTimes)

    //then
    aggregatedResponseTimes.collect().toList should contain theSameElementsAs List(
      ("127.0.0.2", ArrayBuffer(350, 200)),
      ("191.0.0.1", ArrayBuffer(10)),
      ("127.0.0.1", ArrayBuffer(20, 24)))


  }
}

case class MetricsData(host: String, responseTime: Long)

case class AverageResponseTime()
