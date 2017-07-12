package com.malaska.spark.training.streaming.dstream.sessionization

import java.net.Socket
import java.io.OutputStreamWriter

object SessionDataSocketSender {
  
  val eol = System.getProperty("line.separator");  
  
  def main(args: Array[String]) {
    if (args.length == 0) {
        println("SessionDataSocketSender {host} {port} {loops} {waitTime}");
        return;
    }
    
    val host = args(0)
    val port = args(1).toInt
    val loops = args(2).toInt
    val waitTime = args(3).toInt
    
    val socket = new Socket(host, port)
    
    val writer = new OutputStreamWriter(socket.getOutputStream(), "UTF-8")
    
    for (i <- 1 to loops) {
      writer.write(SessionDataGenerator.getNextEvent + eol)
      wait(waitTime)
    }
    
    writer.close
           
  }
}