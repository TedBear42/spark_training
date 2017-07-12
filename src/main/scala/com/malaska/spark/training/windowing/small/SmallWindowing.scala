package com.malaska.spark.training.windowing.small

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Basic example of small windowing.  Where the thing you are windowing
  * is small enough to be in memory.  Normally less then 50k records per key.
  */
object SmallWindowing {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {

    val jsonPath = args(0)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val jsonDf = sparkSession.read.json(jsonPath)

    val timeDifRdd = jsonDf.rdd.map(row => {
      val group = row.getAs[String]("group")
      val time = row.getAs[Long]("time")
      val value = row.getAs[Long]("value")
      //(key  , value)
      (group, (time, value))
    }).groupByKey().flatMap{case (group, records) =>

      var lastValue = 0l

      val localList = records.toSeq
      println("localList.size:" + localList.size)
      localList.sortBy(_._1).map{case (time, value) =>
        val dif = value - lastValue
        lastValue = value
        (group, time, value, dif)
      }
    }

    timeDifRdd.take(10).foreach(r => {
      println(r)
    })

    sparkSession.stop()
  }
}
