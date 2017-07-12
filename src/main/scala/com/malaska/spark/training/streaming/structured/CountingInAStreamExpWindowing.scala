package com.malaska.spark.training.streaming.structured

import com.malaska.spark.training.streaming.{Message, MessageBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, sum, window}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OutputMode


object CountingInAStreamExpWindowing {
  def main(args:Array[String]): Unit = {
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

      val messageDsDStream = socketLines.as[String].map(line => {
        MessageBuilder.build(line)
      }).as[Message]

      val tickerCount = messageDsDStream.withColumn("eventTime", $"tradeTs".cast("timestamp"))
        .groupBy(window($"eventTime", "20 seonds", "5 seonds"))
            .agg(sum($"price") as "total_price", count($"price") as "price_count")

      val ticketOutput = tickerCount.writeStream.outputMode(OutputMode.Complete())
        .format("Console")
        .start()


      ticketOutput.awaitTermination()
    }
  }
}
