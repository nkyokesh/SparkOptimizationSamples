package com.hackprotech

import org.apache.spark.SparkContext

object HelloWorldSpark extends App {
  val sparkContext = new SparkContext(master = "local", appName = "HelloWorld")
  val sourceRDD = sparkContext.textFile("/Users/nkyokesh/Documents/EMR/test1.csv")
  sourceRDD.take(3).foreach(println)

}

