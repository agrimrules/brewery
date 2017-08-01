package com.a10a.spark.brewery

import com.mongodb.spark._
import org.apache.spark.sql.SparkSession


object SparkRDD {
  def main(args: Array[String]) = {

      val sc = SparkSession.builder()
                  .master("local")
                  .appName("Brewery")
                  .config("spark.mongodb.input.uri", "")
                  .config("spark.mongodb.output.uri", "")
                  .getOrCreate()
      val rdds = MongoSpark.load(sc)
      val lonestar = rdds.filter(ds => ds.getAs("name").toString == "Lonestar")
      MongoSpark.save(lonestar)
  }

}
