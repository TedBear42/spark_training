package com.malaska.spark.training.timeseries

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TumblingWindows {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val leadLagJson = args(0)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .enableHiveSupport()
        .getOrCreate()
    }
    println("---")

    import sparkSession.implicits._

    val leadLag = sparkSession.read.json(leadLagJson).as[JsonLeadLag]

    leadLag.createOrReplaceTempView("leadlag")

    sparkSession.sql("select * from leadlag").collect().foreach(println)

    val leadLagDf = sparkSession.sql("SELECT " +
      "group, " +
      "round(ts / 3), " +
      "avg(value), " +
      "max(value), " +
      "min(value) " +
      "FROM leadlag " +
      "group by 1,2")

    leadLagDf.collect().foreach(println)

  }
}