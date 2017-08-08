package com.a10a.spark.brewery


import com.mongodb.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import com.mongodb.spark.config.WriteConfig


object SparkRDD {
  def main(args: Array[String]) = {
      val (inmongo, outmongo) = utils.getMongoConf()
      val sc = SparkSession.builder()
                  .master("local[*]")
                  .appName("Brewery")
                  .config("spark.mongodb.input.uri", inmongo)
                  .config("spark.mongodb.output.uri", outmongo)
                  .getOrCreate()
    val rdds = MongoSpark.load(sc)
    val beers = rdds.select(rdds("name")).distinct.collect()
    beers.foreach(beer => {
      val mapped = {
        rdds.filter(rd => rd.getAs("name").toString == beer.get(0).toString)
      }
      println(mapped)
      val trimmed = mapped.drop("__v").drop("_id").withColumn("Beer Name", concat(mapped.col("brand"), lit(" "), mapped.col("name")))
        .drop("brand").drop("name")
      val formatted: DataFrame = trimmed.select("ts","Beer Name", "percentage").distinct().sort("ts")
      formatted.show()
      // Do machine learning on formatted data
    })

    println("done")
  }

}
