package com.malaska.spark.training.streaming.structured

import com.malaska.spark.training.streaming.{Message, MessageBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


object CountingInAStreamExpWindowing {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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
        .master("local[5]")
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .master("local[5]")
        .getOrCreate()
    }

    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    val messageDsDStream = socketLines.as[(String, Timestamp)].map(line => {
      MessageBuilder.build(line._1, line._2)
    }).filter(r => r != null).as[Message]


    val tickerCount = messageDsDStream.withColumn("eventTime", $"tradeTs".cast("timestamp"))
      .withWatermark("eventTime", "30 seconds")
      .groupBy(window($"eventTime", "30 seconds", "5 seconds"), $"ticker")
      .agg(max($"tradeTs") as "max_time", sum($"price") as "total_price", avg($"price") as "avg_price", count($"price") as "number_of_trades")//.orderBy("window")


    val ticketOutput = tickerCount.writeStream
      .format("Console")
      .option("checkpointLocation", checkpointFolder)
      .outputMode("update")
      //.outputMode("complete")
      .format("console")
      .option("truncate", false)
      .option("numRows", 40)
      .start()

    ticketOutput.awaitTermination()
  }

}
