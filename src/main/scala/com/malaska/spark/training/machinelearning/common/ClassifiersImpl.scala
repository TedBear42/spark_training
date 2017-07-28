package com.malaska.spark.training.machinelearning.common

import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, LogisticRegression, NaiveBayes}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql._

object ClassifiersImpl {
  def logisticRegression(trainingLabeledPointDf: DataFrame,
                         testPercentage:Double): Unit = {
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val splits = trainingLabeledPointDf.randomSplit(Array(testPercentage, 1-testPercentage))

    val model = mlr.fit(splits(0))

    val trainTransformed = model.transform(splits(1))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(trainTransformed)
    println("Test set accuracy of logisticRegression = " + accuracy)

    //println(model)
  }

  def gbtClassifer(trainingLabeledPointDf: DataFrame,
                   testPercentage:Double): Unit = {
    val gbt = new GBTClassifier()

    val splits = trainingLabeledPointDf.randomSplit(Array(testPercentage, 1-testPercentage))

    val model = gbt.fit(splits(0))

    val trainTransformed = model.transform(splits(1))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(trainTransformed)
    println("Test set accuracy of gbtClassifier = " + accuracy)

    //println(model)
    //println(model.toDebugString)
  }

  def randomForestRegressor(trainingLabeledPointDf: DataFrame,
                            impurity:String,
                            maxDepth:Int,
                            maxBins:Int,
                            testPercentage:Double): Unit = {
    val rf = new RandomForestRegressor()

    rf.setImpurity(impurity)
    rf.setMaxDepth(maxDepth)
    rf.setMaxBins(maxBins)

    val splits = trainingLabeledPointDf.randomSplit(Array(testPercentage, 1-testPercentage))

    val model = rf.fit(splits(0))
    val trainTransformed = model.transform(splits(1))

    /*
    trainTransformed.take(10).foreach(r => {
      println(r)
    })
    */

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val accuracy = evaluator.evaluate(trainTransformed)
    println("Test set accuracy of RandomForest:" + impurity + " = " + accuracy)

    println(model)
    println(model.toDebugString)
  }

  def decisionTree(trainingLabeledPointDf: DataFrame,
                   impurity:String,
                   maxDepth:Int,
                   maxBins:Int,
                   testPercentage:Double): Unit = {

    val dt = new DecisionTreeClassifier
    dt.setMaxDepth(maxDepth)
    dt.setMaxBins(maxBins)
    dt.setImpurity(impurity)

    val splits = trainingLabeledPointDf.randomSplit(Array(testPercentage, 1-testPercentage))

    val model = dt.fit(splits(0))

    val trainTransformed = model.transform(splits(1))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(trainTransformed)
    println("Test set accuracy of DecisionTree:" + impurity + " = " + accuracy)

    println(model)
    println(model.toDebugString)
  }

  def naiveBayerTest(trainingLabeledPointDf: DataFrame,
                     testPercentage:Double): Unit = {
    val nb = new NaiveBayes

    val splits = trainingLabeledPointDf.randomSplit(Array(testPercentage, 1-testPercentage))

    val model = nb.fit(splits(0))

    val trainTransformed = model.transform(splits(1))

    /*
    trainTransformed.take(10).foreach(r => {
      println(r)
    })
    */

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(trainTransformed)
    println("Test set accuracy of NaiveBayer = " + accuracy)
  }
}
