//import com.mongodb.spark.MongoSpark
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkRDD {
  def main(args: Array[String]) = {
      val conf = new SparkConf().setAppName("Brewery2").setMaster("local[4]")
      val sc = new SparkContext(conf)
      val numbers = 1 to 10
      println(sc.getConf)
      val rdd = sc.parallelize(numbers)
      rdd.foreach(println)
  }

}
