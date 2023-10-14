

import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class IterateWithMapPartition extends AnyFunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use iterator-to-iterator") {
    //given
    val data = spark
      .parallelize(List(
        UserTransaction("a", 100),
        UserTransaction("b", 101),
        UserTransaction("a", 202),
        UserTransaction("b", 1),
        UserTransaction("c", 55)
      )
      ).keyBy(_.userId)
      .partitionBy(new Partitioner {
        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
          key.hashCode % 3
        }
      })
    // create 3 partitions explicitly by taking the key value(string so type is Any) and find the modulus.
    println(data.partitions.length)

    //when
    val res = data.mapPartitions[Long](iter =>
      iter.map(_._2).map(_.amount)
    ).collect().toList
    // using mapPartition take the 2 values from the pairedRDD and put in the list.
    //then verify the result is correct and the reiterate transformation with mapPartition achieved.
    res should contain theSameElementsAs List(55, 100, 202, 101, 1)
  }
}
