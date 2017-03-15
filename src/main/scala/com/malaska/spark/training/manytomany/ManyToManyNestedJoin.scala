package com.malaska.spark.training.manytomany

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object ManyToManyNestedJoin {
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

      val resultList = new mutable.MutableList[(Long, NestedCount)]

      actions.foreach(a => {
        val aValue = a.getAs[Long]("action")
        val aNestedCount = new NestedCount
        actions.foreach(b => {
          val bValue = b.getAs[Long]("action")
          if (aValue < bValue) {
            aNestedCount.+=(bValue, 1)
          }
        })
        resultList.+=((aValue, aNestedCount))
      })
      resultList.toSeq
    }).reduceByKey((a, b) => a + b)

      //.reduceByKey(_ + _)

    nGramWordCount.collect().foreach(r => {
      println(" - " + r)
    })
  }
}


//1,2
//1,3
//1,4

//1 (2, 3, 4)

class NestedCount() extends Serializable{

  val map = new mutable.HashMap[Long, Long]()

  def += (key:Long, count:Long): Unit = {
    val currentValue = map.getOrElse(key, 0l)
    map.put(key, currentValue + count)
  }

  def + (other:NestedCount): NestedCount = {
    val result = new NestedCount

    other.map.foreach(r => {
      result.+=(r._1, r._2)
    })
    this.map.foreach(r => {
      result.+=(r._1, r._2)
    })
    result
  }

  override def toString(): String = {
    val stringBuilder = new StringBuilder
    map.foreach(r => {
      stringBuilder.append("(" + r._1 + "," + r._2 + ")")
    })
    stringBuilder.toString()
  }
}
