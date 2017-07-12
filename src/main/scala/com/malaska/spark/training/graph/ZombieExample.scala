package com.malaska.spark.training.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by tmalaska on 7/10/17.
  */
object ZombieExample {
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

    val zombieResults = graph.pregel[Long](0, 30, EdgeDirection.Either)(
      (vertexId, zombieState, message) => {
        if (message > 0 && !zombieState.isZombie) {
          new ZombieStats(true, message)
        } else {
          zombieState
        }
      }, triplet => {
        if (triplet.srcAttr.isZombie && !triplet.dstAttr.isZombie) {
          Iterator((triplet.dstId, triplet.srcAttr.lengthOfLife + 1l))
        } else if (triplet.dstAttr.isZombie && !triplet.srcAttr.isZombie) {
          Iterator((triplet.srcId, triplet.dstAttr.lengthOfLife + 1l))
        } else {
          Iterator.empty
        }
      }, (a, b) => Math.min(a, b))

    println("ZombieBite")
    zombieResults.vertices.collect().sortBy(r => r._1).foreach(r => {
      println("vertexId:" + r._1 + ",ZobmieStat:" + r._2)
    })

    sparkSession.stop()
  }
}

case class ZombieStats (isZombie:Boolean, lengthOfLife:Long)
