package com.malaska.spark.training.nested

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NestedDataSetExample {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val jsonPath = args(0)

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    val jsonDf = spark.read.json(jsonPath)

    jsonDf.foreach(row => {
      val group = row.getAs[String]("group")
      println("row:" + row)
    })

    val dataset = spark.read.json(jsonPath).as[GroupBean]

    dataset.foreach(row => {
      val group = row.group
      row.time
      println("dts:" + row)
    })

    dataset.printSchema()

    dataset.toDF().createOrReplaceTempView("case_table")

    spark.sql("select * from case_table").rdd.foreach(println)

    spark.stop()
  }
}

//{"group":"A", "time":5, "value":3, "nested":[{"col":1}, {"col":2}]}

case class GroupBean(group:String, time:Long, value:Long, nested:Array[NestedBean]) extends Serializable {
  override def toString():String = {
    "GroupBean(" + group + "," + time + "," + value + ",NestedBean(" + nested.mkString(",") + "))"
  }
}


case class NestedBean(col1:Long, col2:Long) {
  override def toString():String = {
    "(" + col1 + "," + col2 + ")"
  }
}
