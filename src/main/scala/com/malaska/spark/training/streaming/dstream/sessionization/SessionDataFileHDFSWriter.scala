package com.malaska.spark.training.streaming.dstream.sessionization

import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.OutputStreamWriter
import org.apache.hadoop.fs.Path
import java.util.Random

object SessionDataFileHDFSWriter {
  
  val eol = System.getProperty("line.separator");  
  
  def main(args: Array[String]) {
    if (args.length == 0) {
        println("SessionDataFileWriter {tempDir} {distDir} {numberOfFiles} {numberOfEventsPerFile} {waitBetweenFiles}");
        return;
    }
    val conf = new Configuration
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    
    val fs = FileSystem.get(new Configuration)
    val rootTempDir = args(0)
    val rootDistDir = args(1)
    val files = args(2).toInt
    val loops = args(3).toInt
    val waitBetweenFiles = args(4).toInt
    val r = new Random
    for (f <- 1 to files) {
      val rootName = "/weblog." + System.currentTimeMillis()
      val tmpPath = new Path(rootTempDir + rootName + ".tmp")
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath)))
      
      print(f + ": [")
      
      val randomLoops = loops + r.nextInt(loops)
      
      for (i <- 1 to randomLoops) {
        writer.write(SessionDataGenerator.getNextEvent + eol)
        if (i%100 == 0) {
          print(".")
        }
      }
      println("]")
      writer.close
      
      val distPath = new Path(rootDistDir + rootName + ".dat")
      
      fs.rename(tmpPath, distPath)
      Thread.sleep(waitBetweenFiles)
    }
    println("Done")
  }
}