package com.malaska.spark.training.machinelearning.titanic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SomeSQLOnTitanic {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    def main (args:Array[String]): Unit = {
      val testFile = args(0)
      val trainFile = args(1)

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

      //Load Data
      val trainDs = sparkSession.read.option("header", "true")
        .option("charset", "UTF8")
        .option("delimiter",",")
        .csv(trainFile)

      trainDs.createOrReplaceTempView("train")

      println("Sex -> Servived")
      sparkSession.sql("select Sex, sum(Survived), count(*), (sum(Survived)/count(*)) from train group by Sex").collect().foreach(println)

      println("Cabin -> Servived")
      sparkSession.sql("select substring(Cabin,1,1), sum(Survived), count(*), (sum(Survived)/count(*)) from train group by 1 order by 1").collect().foreach(println)

      println("Age -> Servived")
      sparkSession.sql("select round(cast(Age as Int) / 10) as age_block, sum(Survived), count(*), (sum(Survived)/count(*)) from train group by 1 order by 1").collect().foreach(println)

      println("PClass -> Servived")
      sparkSession.sql("select pclass, sum(Survived), count(*), (sum(Survived)/count(*)) from train group by pclass order by 1").collect().foreach(println)

      println("Embarked -> Servived")
      sparkSession.sql("select Embarked, sum(Survived), count(*), (sum(Survived)/count(*)) from train group by Embarked order by 1").collect().foreach(println)

      println("Fare -> Servived")
      sparkSession.sql("select round((Fare / 10)), sum(Survived), count(*), (sum(Survived)/count(*)) from train group by 1 order by 1").collect().foreach(println)

      println("Servived -> Servived")
      sparkSession.sql("select sum(Survived), count(*) from train order by 1").collect().foreach(println)

      sparkSession.stop()
  }
}
