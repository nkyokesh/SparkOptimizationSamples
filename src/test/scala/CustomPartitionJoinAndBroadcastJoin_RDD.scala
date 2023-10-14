
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CustomPartitionJoinAndBroadcastJoin_RDD extends AnyFunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  spark.setLogLevel("WARN")
  test("should use custom partitioner while join") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val personsDataPartitioner = transactions.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(persons.partitions.length)
    }

    // we can use the custom partitioner in inner join, when any one of the join table partition is predictable in RDD
    // when creating the hashpartitioner, we get the partition length of one table that will be joined with anther table
    val res = persons.join(transactions, personsDataPartitioner).collect().toList

    res should contain theSameElementsAs
      List((2, ("Michael", "dog")), (1, ("Tom", "bag")))
  }

  test("can broadcast small data set to every executor and join in-memory") {
    //given
    val smallDataSet = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val hugeDataSet = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when broadcast small rdd to all executors
    val smallInMemoryDataSet = spark.broadcast(smallDataSet.collectAsMap())

    //then join will not need to do shuffle
    val res = hugeDataSet.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) => smallInMemoryDataSet.value.get(k) match {
          case None => Seq.empty
          case Some(v2) => Seq((k, (v1, v2)))
        }
      }
    })

    res.collect().toList should contain theSameElementsAs
      List((2, ("Michael", "dog")), (1, ("Tom", "bag")))
  }

}


