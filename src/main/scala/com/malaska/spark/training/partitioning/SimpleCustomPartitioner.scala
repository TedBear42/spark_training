package com.malaska.spark.training.partitioning

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object SimpleCustomPartitioner {
  def main(args:Array[String]): Unit = {

    val jsonPath = args(0)
    val partitions = args(1).toInt

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val jsonDf = sparkSession.read.json(jsonPath)

    val partitionedRdd = jsonDf.rdd.map(row => {
      val group = row.getAs[String]("group")
      val time = row.getAs[Long]("time")
      val value = row.getAs[Long]("value")
      ((group, time), value) //this a tuple with in a tuple
    }).repartitionAndSortWithinPartitions(new SimpleCustomPartitioner(partitions))

    val pairRdd = jsonDf.rdd.map(row => {
      val group = row.getAs[String]("group")
      val time = row.getAs[Long]("time")
      val value = row.getAs[Long]("value")
      ((group, time), value) //this a tuple with in a tuple
    })

    pairRdd.reduceByKey(_ + _, 100)
    pairRdd.reduceByKey(new SimpleCustomPartitioner(partitions), _ + _)


    partitionedRdd.collect().foreach(r => {
      println(r)
    })

    sparkSession.stop()
  }
}

class SimpleCustomPartitioner(numOfParts:Int) extends Partitioner {
  override def numPartitions: Int = numOfParts

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, Long)]
    Math.abs(k._1.hashCode) % numPartitions
  }
}
