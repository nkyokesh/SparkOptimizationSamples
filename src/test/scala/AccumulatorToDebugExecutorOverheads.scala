
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite



class AccumulatorToDebugExecutorOverheads extends AnyFunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use accumulator for counter") {
    //given
    val input = spark.makeRDD(List("this", "is", "a", "sentence"))
    val operationsCounter = spark.longAccumulator("operations_counter")
    spark.setLogLevel("WARN")
    //when
    val transformed = input
      .map { x => operationsCounter.add(1); x.toUpperCase }
      //warning: if spark executor fails during transformation, it will re-try:
      //because of that counter will be incremented x2 for partitions that
      //were on failed executor and based on this, we can identify if any task's computation is taking more time,
      // like in this case, there are 4 times of computation supposed to be occurred in the map transformation. So, counters are
      //not used to calculate realtime values.
      .collect()

    //then
    assert(operationsCounter.count == 4)
  }
}

