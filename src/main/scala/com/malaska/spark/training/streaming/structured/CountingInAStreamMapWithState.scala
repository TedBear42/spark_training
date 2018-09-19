package com.malaska.spark.training.streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object CountingInAStreamMapWithState {
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

    val messageDs = socketLines.as[String].
      flatMap(line => line.toLowerCase().split(" ")).
      map(word => WordCountEvent(word, 1))

    // Generate running word count
    val wordCounts = messageDs.groupByKey(tuple => tuple.word).
      mapGroupsWithState[WordCountInMemory, WordCountReturn](GroupStateTimeout.ProcessingTimeTimeout) {

      case (word: String, events: Iterator[WordCountEvent], state: GroupState[WordCountInMemory]) =>
        var newCount = if (state.exists) state.get.countOfWord else 0

        events.foreach(tuple => {
          newCount += tuple.countOfWord
        })

        state.update(WordCountInMemory(newCount))

        WordCountReturn(word, newCount)
    }

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

case class WordCountEvent(word:String, countOfWord:Int) extends Serializable {

}

case class WordCountInMemory(countOfWord: Int) extends Serializable {
}

case class WordCountReturn(word:String, countOfWord:Int) extends Serializable {

}
