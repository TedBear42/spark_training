package com.malaska.spark.training.streaming.structured

import com.malaska.spark.training.streaming.{Message, MessageBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by tmalaska on 7/1/17.
  */
object EnrichmentInAStream {
  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    var checkpointDir = args(2)

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
      .option("checkpointLocation", checkpointDir)
      .load()

    val messageDs = socketLines.as[String].map(line => {
      MessageBuilder.build(line)
    }).as[Message]

    val upperMessageDs = messageDs.map(message => {
      message.toString.toUpperCase()
    }).as[String]

    upperMessageDs.foreachPartition(messageIt => {
      //make connection to storage layer
      // May use static connection
      messageIt.foreach(message => {
        //write to storage location
      })
    })

    val messageOutput = upperMessageDs.writeStream.outputMode(OutputMode.Complete())
      .start()



    messageOutput.awaitTermination()
  }
}
