package com.naresh.org

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount
{
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    // get threshold

    // read in text file and split each document into words
    val tokenized = sc.textFile("/user/nd2629/retail/orders")

   // println("***************"+tokenized.count()+"&&&&&&&&&&&")
    tokenized.saveAsTextFile("/user/nd2629/retail")
  }

}
