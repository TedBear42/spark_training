package com.malaska.spark.training.nested

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable

object JsonNestedExample {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val jsonPath = args(0)

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
    println("---")

    val jsonDf = sparkSession.read.json(jsonPath)

    val localJsonDf = jsonDf.collect()

    println("--Df")
    jsonDf.foreach(row => {
      println("row:" + row)
    })
    println("--local")
    localJsonDf.foreach(row => {
      println("row:" + row)
    })

    jsonDf.createOrReplaceTempView("json_table")

    println("--Tree Schema")
    jsonDf.schema.printTreeString()
    println("--")
    jsonDf.write.saveAsTable("json_hive_table")

    jsonDf.write.saveAsTable("foobar")

    sparkSession.sqlContext.sql("select * from json_hive_table").take(10).foreach(println)

    println("--")
    sparkSession.sqlContext.sql("select group, explode(nested) as n1 from json_table").createOrReplaceTempView("unnested")

    sparkSession.sqlContext.sql("select * from unnested").printSchema()

    sparkSession.sqlContext.sql("select * from unnested").rdd.foreach(println)

    sparkSession.sqlContext.sql("select group, a.col1, a.col2 from json_table LATERAL VIEW explode(nested) as a").printSchema()

    sparkSession.sqlContext.sql("select group, a.col1, a.col2 from json_table LATERAL VIEW explode(nested) as a").rdd.foreach(println)
    println("---")
/*
    jsonDf.rdd.map(row => {
      val fields = row.schema.fields

      val flattedMap = new mutable.HashMap[(String, DataType), mutable.MutableList[Any]]()

      populatedFlattedHashMap(row, row.schema, fields, flattedMap, "")

      flattedMap
    }).foreach(r => {
      print("{")
      r.foreach(m => {
        print("\"" + m._1 + "\":(")
        m._2.foreach(n => {
          print(n + ",")
        })
        print("),")
      })
      println("}")
    })
*/
    sparkSession.stop()
  }

  def populatedFlattedHashMap(row:Row,
                              schema:StructType,
                              fields:Array[StructField],
                              flattedMap:mutable.HashMap[(String, DataType), mutable.MutableList[Any]],
                              parentFieldName:String): Unit = {
    fields.foreach(field => {

      println("field:" + field.dataType)
      if (field.dataType.isInstanceOf[ArrayType]) {
        val elementType = field.dataType.asInstanceOf[ArrayType].elementType
        if (elementType.isInstanceOf[StructType]) {
          val childSchema = elementType.asInstanceOf[StructType]

          val childRow = Row.fromSeq(row.getAs[mutable.WrappedArray[Any]](field.name).toSeq)

          populatedFlattedHashMap(childRow, childSchema, childSchema.fields, flattedMap, parentFieldName + field.name + ".")
        }
      } else {
        val fieldList = flattedMap.getOrElseUpdate((parentFieldName + field.name, field.dataType), new mutable.MutableList[Any])
        fieldList.+=:(row.getAs[Any](schema.fieldIndex(field.name)))
      }

    })
  }
}
