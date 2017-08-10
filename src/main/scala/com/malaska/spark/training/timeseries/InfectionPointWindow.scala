package com.malaska.spark.training.timeseries

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InfectionPointWindow {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val inflectionPointJson = args(0)

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

    val inflectionPointDs = sparkSession.read.json(inflectionPointJson).as[JsonInfectionPoint]

    inflectionPointDs.createOrReplaceTempView("inflection_point")

    sparkSession.sql("select * from inflection_point").collect().foreach(println)

    val leadLagDf = sparkSession.sql("SELECT " +
      "group, ts, " +
      "value as v_now, " +
      "AVG(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg, " +
      "Min(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg, " +
      "Max(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg " +
      "FROM inflection_point " +
      "where event_type = 'inflection'")

    leadLagDf.collect().foreach(println)

  }
}

case class JsonInfectionPoint(group:String, ts:Long, value:Long, event_type:String)
