package com.malaska.spark.training.manytomany

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object ManyToManyNormalJoin {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val jsonPath = args(0)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    val jsonDf = sparkSession.read.json(jsonPath)

    val nGramWordCount = jsonDf.rdd.flatMap(r => {
      val actions = r.getAs[mutable.WrappedArray[Row]]("actions")

      val resultList = new mutable.MutableList[((Long, Long), Int)]

      actions.foreach(a => {
        val aValue = a.getAs[Long]("action")
        actions.foreach(b => {
          val bValue = b.getAs[Long]("action")
          if (aValue < bValue) {
            resultList.+=(((aValue, bValue), 1))
          }
        })
      })
      resultList.toSeq
    }).reduceByKey(_ + _)

    nGramWordCount.collect().foreach(r => {
      println(" - " + r)
    })
  }
}
