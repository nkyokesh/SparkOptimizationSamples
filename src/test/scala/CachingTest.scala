import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class CachingTest extends AnyFunSuite {
  val spark: SparkContext = SparkSession
    .builder().master("local[2]").getOrCreate().sparkContext

  /**
   * In Spark, RDDs are cached in memory by default.
   * To avoid calculating data again we can use cache() method
   */
  test("when not caching intermediate result it will take longer time") {
    //given
    val users: RDD[(VertexId, String)] =
      spark.parallelize(Array(
        (1L, "a"),
        (2L, "b"),
        (3L, "c"),
        (4L, "d")
      ))


    val relationships =
      spark.parallelize(Array(
        Edge(1L, 2L, "friend"),
        Edge(1L, 3L, "friend"),
        Edge(2L, 4L, "wife")
      ))

    val graph = Graph(users, relationships)

    //when
    val res = graph.mapEdges(e => e.attr.toUpperCase)
    val res2 = graph.mapVertices((_, att) => att.toUpperCase()) //it will load graph again

    res.edges.collect().toList
    res2.edges.collect().toList
  }

  test("when use caching on intermediate result will be calculated faster") {
    //given
    val users: RDD[(VertexId, String)] =
      spark.parallelize(Array(
        (1L, "a"),
        (2L, "b"),
        (3L, "c"),
        (4L, "d")
      ))


    val relationships =
      spark.parallelize(Array(
        Edge(1L, 2L, "friend"),
        Edge(1L, 3L, "friend"),
        Edge(2L, 4L, "wife")
      ))

    val graph = Graph(users, relationships).cache()

    //when
    val res = graph.mapEdges(e => e.attr.toUpperCase).cache()
    val res2 = graph.mapVertices((_, att) => att.toUpperCase()) //it will load graph again


    res.edges.collect().toList
    res2.edges.collect().toList
  }

  test("when use Persist on intermediate result will be calculated faster") {
    //given
    val users: RDD[String] = spark.makeRDD(List("a", "b"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    // persist would be more efficient and avoid the OOM error as cache will store the result set in the memory
    // however, persist will spill over to disk in the same locality of the memory exceeded.

    //when
    val res = users.map(_.toUpperCase)
    val res2 = users.map(_.toLowerCase)

    //then
    res.collect().toList
    res2.collect().toList
  }

}
