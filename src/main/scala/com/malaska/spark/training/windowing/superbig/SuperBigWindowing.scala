package com.malaska.spark.training.windowing.superbig

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object SuperBigWindowing {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val jsonPath = args(0)
    val pageSize = args(1).toInt

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    val jsonDf = spark.read.json(jsonPath)

    import spark.implicits._

    val diffDs = jsonDf.flatMap(row => {
      val group = row.getAs[String]("group")
      val time = row.getAs[Long]("time")
      val value = row.getAs[Long]("value")

      val timePage = time / pageSize

      if (time %  pageSize == 0) { //Am I on the edge of the page
        Seq((timePage, (time, value)), (timePage + 1, (time, value)))
      } else {
        Seq((timePage, (time, value)))
      }
    }).groupByKey(r => r._1).flatMapGroups((k, it) => {
      var lastValue = 0l

      it.toSeq.
        sortBy{case (page, (time, value)) => time}.
        map{case (page, (time, value)) =>
        val dif = value - lastValue
        lastValue = value
        (time, value, dif)
      }
    })

    diffDs.collect().foreach(r => println(" - " + r))

    spark.stop()

  }
}

