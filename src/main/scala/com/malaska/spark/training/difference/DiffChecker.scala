package com.malaska.spark.training.difference

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by tmalaska on 6/18/17.
  */
object DiffChecker {
  def main (args:Array[String]): Unit = {

    val originalRawFileLocation = args(0)
    val revertedRawFileLocation = args(0)

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .getOrCreate()

    val originalRDD = spark.read.textFile(originalRawFileLocation)
        .rdd.map(r => (r, 1))

    val revertedRdd = spark.read.textFile(revertedRawFileLocation)
        .rdd.map(r => (r, -1))

    val misMatches = originalRDD.union(revertedRdd).reduceByKey(_ + _).filter(_._2 > 1)

    misMatches.take(10).foreach(println)
  }
}
