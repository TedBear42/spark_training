package com.malaska.spark.training.machinelearning.golf

import com.malaska.spark.training.machinelearning.common.ClassifiersImpl
import com.malaska.spark.training.machinelearning.titanic.TrainPassenger
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

object PredictGolfing {
  def main(args:Array[String]): Unit = {
    val testFile = args(0)
    val trainFile = args(1)
    val testPercentage = args(2).toDouble

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
      .option("delimiter","\t")
      .csv(trainFile)
      .as[GolfDay]

    val labeledPointRdd = trainDs.rdd.map(golfDay => {
      val outlookSunny = if (golfDay.outlook.equals("sunny")) 1d else 0d
      val outlookRainy = if (golfDay.outlook.equals("rainy")) 1d else 0d
      val outlookOvercast = if (golfDay.outlook.equals("overcast")) 1d else 0d
      val temp = golfDay.temp.toDouble
      val tempHot = if (golfDay.outlook.equals("hot")) 1d else 0d
      val tempMild = if (golfDay.outlook.equals("mild")) 1d else 0d
      val tempCool = if (golfDay.outlook.equals("cool")) 1d else 0d
      val humidityHigh = if (golfDay.outlook.equals("high")) 1d else 0d
      val humidityNormal = if (golfDay.outlook.equals("normal")) 1d else 0d
      val windy = if (golfDay.outlook.equals("true")) 1d else 0d
      val play = if (golfDay.outlook.equals("yes")) 1d else 0d
      val vector: Vector = Vectors.dense(Array(outlookSunny,
        outlookRainy,
        outlookOvercast,
        temp,
        tempHot,
        tempMild,
        tempCool,
        humidityHigh,
        humidityNormal,
        windy))

      (play, vector)
    })

    val labeledPointDf = labeledPointRdd.toDF("passenderId", "features")

    ClassifiersImpl.naiveBayerTest(labeledPointDf, testPercentage)
    ClassifiersImpl.decisionTree(labeledPointDf, "gini", 7, 32, testPercentage)
    ClassifiersImpl.decisionTree(labeledPointDf, "entropy", 7, 32, testPercentage)
    ClassifiersImpl.randomForestRegressor(labeledPointDf, "variance", 5, 32, testPercentage)
    ClassifiersImpl.gbtClassifer(labeledPointDf, testPercentage)
    ClassifiersImpl.logisticRegression(labeledPointDf, testPercentage)
  }
}

case class GolfDay(outlook:String,
                   temp:Int,
                   tempEnum:String,
                   humidity:Int,
                   humidityEnum:String,
                   windyFlag:Boolean,
                   playFlag:Boolean)
