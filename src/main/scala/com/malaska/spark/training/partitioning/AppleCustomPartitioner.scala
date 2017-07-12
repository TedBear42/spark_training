package com.malaska.spark.training.partitioning

import java.util.Random

import org.apache.spark.Partitioner


class AppleCustomPartitioner(numOfParts:Int) extends Partitioner {
  override def numPartitions: Int = numOfParts
  def random = new Random()

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, Long)]
    val ticker = k._1
    if (ticker.equals("apple")) {
      val saltedTicker = ticker + random.nextInt(9)
      Math.abs(saltedTicker.hashCode) % numPartitions
    } else {
      Math.abs(ticker.hashCode) % numPartitions
    }
  }
}
