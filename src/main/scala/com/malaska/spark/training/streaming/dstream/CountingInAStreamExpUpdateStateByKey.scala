package com.malaska.spark.training.streaming.dstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountingInAStreamExpUpdateStateByKey {
  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    val checkpointFolder = args(2)

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

    val ssc = new StreamingContext(sparkSession.sparkContext.getConf, Seconds(1))
    ssc.checkpoint(checkpointFolder)

    val lines = ssc.socketTextStream(host, port.toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1))
      .updateStateByKey((values: Seq[(Int)], state: Option[(Int)]) => {
        var value = state.getOrElse(0)
        values.foreach(i => {
          value += i
        })
        Some(value)
    })

    //words.map(x => (x, 1)).mapWithState()

    wordCounts.print()
    ssc.start()


    ssc.awaitTermination()


  }
}
