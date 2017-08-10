package com.malaska.spark.training.timeseries

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LeadLagExample {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val leadLagJson = args(0)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host", "127.0.0.1")
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
    println("---")

    import sparkSession.implicits._

    val leadLag = sparkSession.read.json(leadLagJson).as[JsonLeadLag]

    leadLag.createOrReplaceTempView("leadlag")

    sparkSession.sql("select * from leadlag").collect().foreach(println)

    val leadLagDf = sparkSession.sql("SELECT " +
      "group, ts, " +
      "value as v_now, " +
      "LEAD(value) OVER (PARTITION BY group ORDER BY ts) as v_after, " +
      "LAG(value)  OVER (PARTITION BY group ORDER BY ts) as v_before " +
      "FROM leadlag")

    leadLagDf.collect().foreach(println)

    leadLagDf.createOrReplaceTempView("leadlag_stage2")

    leadLagDf.printSchema()

    sparkSession.sql("select " +
      "group, ts, v_now, v_after, v_before, " +
      "case " +
      " when v_now < v_after and v_now < v_before then 'valley'" +
      " when v_now > v_after and v_now > v_before then 'peak'" +
      " else 'n/a' " +
      "end " +
      "from leadlag_stage2").collect().foreach(println)
  }
}

case class JsonLeadLag(group:String, ts:Long, value:Long)