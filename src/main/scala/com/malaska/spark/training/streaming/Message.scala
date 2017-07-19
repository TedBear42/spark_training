package com.malaska.spark.training.streaming

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Message (srcUser:String, destUser:String, ticker:String, price:Double, tradeTs:Long) extends Serializable

object MessageBuilder {
  def build(input:String): Message = {
    val parts = input.split(',')

    if (parts.length == 5) {
      Message(parts(0), parts(1), parts(2), parts(3).toDouble, parts(4).toLong)
    } else {
      null
    }

  }

  def build(input:String, ts:Timestamp): Message = {
    val parts = input.split(',')
    if (parts.length == 5) {
      Message(parts(0), parts(1), parts(2), parts(3).toDouble, ts)
    } else {
      null
    }

  }
}