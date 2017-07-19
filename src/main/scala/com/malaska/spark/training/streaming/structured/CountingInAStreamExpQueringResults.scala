package com.malaska.spark.training.streaming.structured

import com.malaska.spark.training.streaming.{Message, MessageBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by tmalaska on 6/25/17.
  */
object CountingInAStreamExpQueringResults {
  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .master("local[3]")
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .master("local[3]")
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

    val tickerCount = messageDsDStream.groupBy("ticker").count()
    val destCount = messageDsDStream.groupBy("destUser").count()

    val ticketOutput = tickerCount.writeStream.outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("ticker_counts")
      .start()

    val destOutput = destCount.writeStream
      .format("memory")
      .queryName("dest_counts")
      .start()


    while (true) {
      println("ticker_counts")
      sparkSession.sql("select * from ticker_counts").collect().foreach(println)
      println("dest_counts")
      sparkSession.sql("select * from dest_counts").collect().foreach(println)
    }
    destOutput.awaitTermination()
    ticketOutput.awaitTermination()
  }
}
