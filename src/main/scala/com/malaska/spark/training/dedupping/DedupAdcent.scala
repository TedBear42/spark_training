package com.malaska.spark.training.dedupping

import org.apache.spark.sql.SparkSession

/**
  * Created by tmalaska on 7/14/17.
  */
object DedupAdcent {
  def main(args:Array[String]): Unit = {

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host","127.0.0.1")
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

    val rdd = sparkSession.sparkContext.parallelize(Array(1, 1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 8, 8, 8), 4)

    val firsts = rdd.mapPartitionsWithIndex((p, it) => {
      var firstValue = if (it.hasNext) { it.next() } else -1
      Seq((p, firstValue)).iterator
    }).collect()

    println("firsts")
    firsts.foreach(println)

    println("partition dedup")
    val insideFilter = rdd.mapPartitionsWithIndex((p, it) => {
      var lastValue:Int = -1
      it.map(r => {
        val result = if (r == lastValue) {
          null
        } else {
          r
        }
        lastValue = r
        result
      }).filter(r => r != null)
    })

    insideFilter.collect().foreach(println)
  }
}
