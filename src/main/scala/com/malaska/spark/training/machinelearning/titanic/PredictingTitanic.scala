package com.malaska.spark.training.machinelearning.titanic

import com.malaska.spark.training.machinelearning.common.ClassifiersImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}

object PredictingTitanic {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main (args:Array[String]): Unit = {
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
      .option("delimiter",",")
      .csv(trainFile)
      .as[TrainPassenger]

    val testDs = sparkSession.read.option("header", "true")
      .option("charset", "UTF8")
      .option("delimiter",",")
      .csv(testFile)
      .as[TestPassenger]

    trainDs.take(10).foreach(println)

    //Featurer Enrichment
    val trainingLabeledPointDf = createLabeledPointDataFrame(sparkSession, trainDs)

    ClassifiersImpl.naiveBayerTest(trainingLabeledPointDf, testPercentage)
    ClassifiersImpl.decisionTree(trainingLabeledPointDf, "gini", 7, 32, testPercentage)
    ClassifiersImpl.decisionTree(trainingLabeledPointDf, "entropy", 7, 32, testPercentage)
    ClassifiersImpl.randomForestRegressor(trainingLabeledPointDf, "variance", 5, 32, testPercentage)
    ClassifiersImpl.gbtClassifer(trainingLabeledPointDf, testPercentage)
    ClassifiersImpl.logisticRegression(trainingLabeledPointDf, testPercentage)

  }




  def createLabeledPointDataFrame(sparkSession: SparkSession, inputDs: Dataset[TrainPassenger]): sql.DataFrame = {

    import sparkSession.implicits._

    val labeledPointRdd = inputDs.rdd.map(testPassenger => {
      val (passengerId: Double, vector: Vector) = createVector(testPassenger.getTrainPassenger())

      (testPassenger.survived.toDouble, vector)
    })

    labeledPointRdd.toDF("label", "features")
  }

  def createTrainDataFrame(sparkSession: SparkSession, inputDs: Dataset[TestPassenger]): sql.DataFrame = {

    import sparkSession.implicits._

    val labeledPointRdd = inputDs.rdd.map(trainPassenger => {
      val (passengerId: Double, vector: Vector) = createVector(trainPassenger)

      (passengerId, vector)
    })

    labeledPointRdd.toDF("passenderId", "features")
  }

  def createVector(ip: TestPassenger): (Double, Vector) = {
    val passengerId = ip.passengerId.toDouble
    val pClass = ip.pclass.toDouble
    val isMan = if (ip.sex.equals("male")) 1.0 else 0.0
    val age = if (ip.age == null) 45.0 else ip.age.toDouble
    val noAge = if (ip.age == null) 1.0 else 0.0
    val isAKid = if (ip.age != null &&  ip.age.toDouble < 10) 1.0 else 0.0
    val isATeen = if (ip.age != null &&  ip.age.toDouble > 12 && ip.age.toDouble < 19) 1.0 else 0.0
    val isAOld = if (ip.age != null &&  ip.age.toDouble > 45) 1.0 else 0.0
    val hasHelper = if (ip.sibSp.toInt > 0) 1.0 else 0.0
    val siblingCount = ip.sibSp.toDouble
    val hasDependent = if (ip.parch.toInt > 0) 1.0 else 0.0
    val depententCount = ip.parch.toDouble
    val hasFamily = if (siblingCount + depententCount > 0) 1.0 else 0.0
    val familyCount = (siblingCount + depententCount).toDouble
    val inUnknownCabin = if (ip.cabin == null || ip.cabin.equals("")) 1.0 else 0.0
    val moreThenOneCabin = if (ip.cabin != null && ip.cabin.contains(' ')) 1.0 else 0.0
    val isCabinA = if (ip.cabin != null && ip.cabin.contains('A')) 1.0 else 0.0
    val isCabinB = if (ip.cabin != null && ip.cabin.contains('B')) 1.0 else 0.0
    val isCabinC = if (ip.cabin != null && ip.cabin.contains('C')) 1.0 else 0.0
    val isCabinD = if (ip.cabin != null && ip.cabin.contains('D')) 1.0 else 0.0
    val isCabinE = if (ip.cabin != null && ip.cabin.contains('E')) 1.0 else 0.0
    val isCabinF = if (ip.cabin != null && ip.cabin.contains('F')) 1.0 else 0.0
    val isCabinT = if (ip.cabin != null && ip.cabin.contains('T')) 1.0 else 0.0
    val isCabinG = if (ip.cabin != null && ip.cabin.contains('G')) 1.0 else 0.0
    val isEmbarkS = if (ip.cabin != null && ip.cabin.contains('S')) 1.0 else 0.0
    val isEmbarkC = if (ip.cabin != null && ip.cabin.contains('C')) 1.0 else 0.0
    val isEmbarkQ = if (ip.cabin != null && ip.cabin.contains('Q')) 1.0 else 0.0
    var fare = ip.fare.toDouble


    val vector = Vectors.dense(Array(isMan,
      age,
      noAge,
      hasHelper,
      //siblingCount,
      hasDependent,
      depententCount,
      hasFamily,
      //familyCount,
      inUnknownCabin,
      moreThenOneCabin,
      isCabinA,
      isCabinB,
      isCabinC,
      isCabinD,
      isCabinE,
      isCabinF,
      isCabinT,
      isCabinG,
      isEmbarkS,
      isEmbarkC,
      isEmbarkQ,
      pClass, //20
      fare)) //21
    (passengerId, vector)
  }
}





