package com.malaska.spark.training.windowing.big

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

/**
  * Big windowing.  This is when you have over 50k records per key or the
  * records can be on bound, but you still have many keys.
  *
  * If you only have one key or very few keys then you will want the
  * super big windowing implementations
  */
object BigWindowing {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val jsonPath = args(0)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    val jsonDf = sparkSession.read.json(jsonPath)

    val timeDifRdd = jsonDf.rdd.map(row => {
      val group = row.getAs[String]("group")
      val time = row.getAs[Long]("time")
      val value = row.getAs[Long]("value")
      ((group, time), value)
    }).repartitionAndSortWithinPartitions(new GroupPartitioner(2)).
      mapPartitions(it => {
        var lastValue = 0l
        var currentGroup = "n/a"
        it.map{ case((group, time), value) =>
          if (!group.equals(currentGroup)) {
            lastValue = 0l
            currentGroup = group
          }
          val dif = value - lastValue
          lastValue = value
          (group, time, value, dif)
        }
      })

    timeDifRdd.collect().foreach(r => {
      println(r)
    })

    sparkSession.stop()
  }
}

/**
  * Our customer partitioner will only partition on the first tuple
  * then it will allow for the sorting on the whole key
  * @param numParts
  */
class GroupPartitioner(val numParts:Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[(String, Long)]._1.hashCode % numPartitions
  }
}
