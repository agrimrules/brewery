package com.a10a.spark.brewery

import org.apache.spark.{SparkConf, SparkContext}


object SparkRDD {
  def main(args: Array[String]) = {
      val conf = new SparkConf().setAppName("Brewery").setMaster("spark://192.168.0.200:7077")
      val sc = new SparkContext(conf)
      val numbers = 1 to 10
      val rdd = sc.parallelize(numbers)
      rdd.foreach(println)
  }

}
