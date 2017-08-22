package com.a10a.spark.brewery


import breeze.linalg.DenseVector
import com.cloudera.sparkts.models.ARIMA
import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit

object SparkRDD {
  def main(args: Array[String]) = {
      val (inmongo, outmongo) = utils.getMongoConf()
      val sc = SparkSession.builder()
                  .master("local[8]")
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
      formatted.show(5)
      val input = Vectors.dense(formatted.select("percentage").rdd.map(r => r(0).toString.toDouble).collect())
      if(input.toArray.distinct.length > 1) {
        val model = ARIMA.fitModel(1, 0, 1, input)
        println("coefficients: " + model.coefficients.mkString(","))
        val predictions = model.forecast(input, 48)
        println(predictions.toArray.toString)
      }
      else {
        val name = formatted.select("Beer Name").collect().head.get(0)
        println(name+" does not show suitable deviation")
      }
      //Not saving predictions for now.
//      val writeConfig = WriteConfig(Map("collection" -> formatted.first().getAs("Beer Name").toString.concat(" Predict"), "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
//      MongoSpark.save(formatted, writeConfig)
    })

    println("done")
  }

}
