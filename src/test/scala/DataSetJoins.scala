

import org.apache.parquet.hadoop.example.ExampleOutputFormat
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DataSetJoins extends AnyFunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("Should inner join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "inner")


    //then
    res.show()
    assert(res.count() == 2)
  }

  test("Should left join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "left_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }


  test("Should right join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "right_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }

  test("Should full outer join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "full_outer")


    //then
    res.show()
    assert(res.count() == 4)
  }


}

/*
Output Example

output tuples are type-safe which is the advantage in dataset joins when compared to dataframe. However, DS joins are jvm objects

+------+--------+
|    _1|      _2|
+------+--------+
|{a, 1}|{a, 100}|
  |{b, 2}|{b, 300}|
  +------+--------+
*/


/*

Source reference: https://databasetown.com/difference-between-dataset-vs-dataframe/

Data Type: A DataFrame is a 2D size-mutable, tabular data structure with rows and columns.
It can hold any data type, whereas a Dataset is a collection of strongly-typed JVM objects, and it is type-safe.

Performance: A DataFrame is generally faster than a Dataset when it comes to performance because the latter uses the Java Virtual Machine (JVM)
and the former uses code generation. DataFrames are implemented on top of RDDs (Resilient Distributed Datasets) and optimized for performance.

API: DataFrames have a wider variety of APIs and are more flexible when it comes to data manipulation,
whereas Datasets have a more limited set of APIs, but they are more concise and expressive.

Type Safety: Datasets provide compile-time type safety, which means that if you try to store an incompatible type in a Dataset,
the code will not compile. DataFrames, on the other hand, are not type-safe and may lead to runtime errors.

Memory Management: DataFrames leverage lazy evaluation, which means that it will not perform any computation until an action is performed on the data.
This allows for better memory management, whereas Datasets perform immediate evaluation and consume more memory.

Use Case: DataFrames are generally used for structured and semi-structured data, whereas Datasets are used for strongly-typed,
object-oriented programming and can handle more complex data structures and operations.

 */