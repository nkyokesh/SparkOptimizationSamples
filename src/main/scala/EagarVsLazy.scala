import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object EagerVsLazy extends App {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  //given
  val input1 = spark.makeRDD(List(1, 2, 3, 4))

  //when apply transformation
  val rdd = input1
    .filter(_ > 1)
    .map(_ * 10)


  //then what? - we are not need the result set, so it is not calculated


  //given
  val input12 = spark.makeRDD(List(1, 2, 3, 4))

  //when apply transformation
  val data1 = input12
    .filter(_ > 1)
    .map(_ * 10)

  //then - to trigger computation we need to issue action

  //expect what? - we are not need the result set, so it is not calculated
  data1.collect().toList //should contain theSameElementsAs
  List(20, 30, 40)



}

