package com.malaska.spark.training.streaming.structured

import com.malaska.spark.training.streaming.{Message, MessageBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

object CountingInAStreamExpGroupBy {
  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)

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

    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    val messageDs = socketLines.as[String].map(line => {
      MessageBuilder.build(line)
    }).as[Message]

    val tickerCount = messageDs.groupBy("ticker", "destUserId").agg(sum($"price"), avg($"price"))

    val destCount = messageDs.groupBy("destUser").count()

    val ticketOutput = tickerCount.writeStream.outputMode(OutputMode.Complete())
      .format("Console")
      .start()
    val destOutput = destCount.writeStream.outputMode(OutputMode.Complete())
      .format("Console")
      .start()



    ticketOutput.awaitTermination()
  }
}
