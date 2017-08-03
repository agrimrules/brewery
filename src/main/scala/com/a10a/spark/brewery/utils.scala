package com.a10a.spark.brewery

import java.io.FileInputStream
import java.util.Properties

object utils {

  def getMongoConf(): (String, String) =
    try {
    val prop = new Properties()
    prop.load(new FileInputStream("./src/main/resources/mongo.conf"))
    (
      prop.getProperty("mongodb.input"),
      prop.getProperty("mongodb.output")
    )
  } catch { case e: Exception =>
    e.printStackTrace()
    sys.exit(1)
  }

}
