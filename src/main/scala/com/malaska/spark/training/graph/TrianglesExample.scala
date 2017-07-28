package com.malaska.spark.training.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object TrianglesExample {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val vertexJsonFile = args(0)
    val edgeJsonFile = args(1)

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

    import sparkSession.implicits._

    val vectorDs = sparkSession.read.json(vertexJsonFile).as[JsonVertex]
    val edgeDs = sparkSession.read.json(edgeJsonFile).as[JsonEdge]

    val vectorRdd:RDD[(VertexId, ZombieStats)] = vectorDs.rdd.map(r => {
      (r.vertex_id.toLong, new ZombieStats(r.is_zombie.equals("yes"), r.time_alive))
    })

    val edgeRdd = edgeDs.rdd.map(r => {
      new Edge[String](r.src, r.dst, r.edge_type)
    })

    val defaultUser = new ZombieStats(false, 0)

    val graph = Graph(vectorRdd, edgeRdd, defaultUser)

    println("TriangleCount")
    graph.triangleCount().vertices.collect().sortBy(r => r._1).foreach(r => {
      println("vertexId:" + r._1 + ",triangleCount:" + r._2)
    })

    graph.pageRank(1.1, 1.1)

    sparkSession.stop()
  }
}





