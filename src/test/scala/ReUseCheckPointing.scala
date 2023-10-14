

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class ReUseCheckPointing extends AnyFunSuite {
  private val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  private val checkpointEnabled = true
  private val storageLevel = StorageLevel.MEMORY_AND_DISK
  spark.setLogLevel("WARN")
  test("should use checkpoint for re-usability of RDD") {
    //given
    val sortedRDD = spark.makeRDD(List(1, 2, 5, 77, 888))
    //explicitly set persistence storage level to memory_and_disk.
    if (storageLevel != StorageLevel.NONE) {
      sortedRDD.persist(storageLevel)
    }
    //setting checkpoint directory to /tmp to recover the computation from the point it failed again.
    if (checkpointEnabled) {
      sortedRDD.sparkContext.setCheckpointDir("/tmp/checkpoint")
      sortedRDD.checkpoint()
    }

    //when
    performALotOfExpensiveComputations(sortedRDD)

    //then
    sortedRDD.collect().toList
  }

  def performALotOfExpensiveComputations(sortedRDD: RDD[Int]): Unit = {
    //....
    sortedRDD.count()
    //In case any failure occurred at this stage, as already sortedRDD is actioned, it will restart/recover from this
    //checkpoint and perform the other action below,
    sortedRDD.collect()
  }
}


