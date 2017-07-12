package com.malaska.spark.training.streaming

case class Message (srcUser:String, destUser:String, ticker:String, price:Double, tradeTs:Long)

object MessageBuilder {
  def build(input:String): Message = {
    val parts = input.split(',')
    Message(parts(0), parts(1), parts(2), parts(3).toDouble, parts(4).toLong)
  }
}