package com.malaska.spark.training.nested

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object NestedTableExample {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.driver.host","127.0.0.1")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql("create table IF NOT EXISTS nested_empty " +
      "( A int, " +
      "  B string, " +
      "  nested ARRAY<STRUCT< " +
      "     nested_C: int," +
      "     nested_D: string" +
      "  >>" +
      ") ")

    val rowRDD = spark.sparkContext.
      parallelize(Array(
        Row(1, "foo", Seq(Row(1, "barA"),Row(2, "bar"))),
        Row(2, "foo", Seq(Row(1, "barB"),Row(2, "bar"))),
        Row(3, "foo", Seq(Row(1, "barC"),Row(2, "bar")))))

    val emptyDf = spark.sql("select * from nested_empty limit 0")

    val tableSchema = emptyDf.schema

    val populated1Df = spark.sqlContext.createDataFrame(rowRDD, tableSchema)

    println("----")
    populated1Df.collect().foreach(r => println(" emptySchemaExample:" + r))

    val nestedSchema = new StructType()
      .add("nested_C", IntegerType)
      .add("nested_D", StringType)

    val definedSchema = new StructType()
      .add("A", IntegerType)
      .add("B", StringType)
      .add("nested", ArrayType(nestedSchema))

    val populated2Df = spark.sqlContext.createDataFrame(rowRDD, definedSchema)

    println("----")
    populated1Df.collect().foreach(r => println(" BuiltExample:" + r))

    spark.stop()
  }
}
