package com.a10a.spark.brewery


import org.apache.spark.ml.tuning.TrainValidationSplit
import com.cloudera.sparkts.models.ARIMA
import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.FloatType

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
      val trimmed = mapped.drop("__v").drop("_id").withColumn("Beer Name", concat(mapped.col("brand"), lit(" "), mapped.col("name")))
        .drop("brand").drop("name")
      val formatted: DataFrame = trimmed.select("ts","Beer Name", "percentage").distinct().sort("ts")
      val input = Vectors.dense(formatted.select("percentage").rdd.map(r => r(0).toString.toDouble).collect())
      if(input.toArray.distinct.length > 1) {
        val train = Vectors.dense(input.toArray.take((input.size * 0.8).asInstanceOf[Int]))
        val validate = Vectors.dense(input.toArray.takeRight((input.size * 0.2).asInstanceOf[Int]))
        val model = ARIMA.fitModel(1, 0, 1, train)
        println("coefficients: " + model.coefficients.mkString(","))
        val predictions = sc.sparkContext.parallelize(model.forecast(train, validate.size + 1).toArray.drop(0).toList)
        val res = formatted.rdd.zip(predictions).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
        val op = sc.createDataFrame(res, formatted.schema.add("prediction", FloatType))
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
