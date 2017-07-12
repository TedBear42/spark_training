package com.malaska.spark.training.streaming.dstream.sessionization

import java.io.BufferedWriter
import java.io.FileWriter

object SessionDataFileWriter {
  
  val eol = System.getProperty("line.separator");  
  
  def main(args: Array[String]) {
    if (args.length == 0) {
        println("SessionDataFileWriter {numberOfRecords} {outputFile} ");
        return;
    }
    
    val writer = new BufferedWriter(new FileWriter(args(1)))
    val loops = args(0).toInt
    
    for (i <- 1 to loops) {
      writer.write(SessionDataGenerator.getNextEvent + eol)
    }
    
    writer.close
  }
}