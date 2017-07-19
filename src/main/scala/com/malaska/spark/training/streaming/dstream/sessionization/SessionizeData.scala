package com.malaska.spark.training.streaming.dstream.sessionization

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.io.LongWritable
import java.text.SimpleDateFormat
//import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
//import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.collection.immutable.HashMap
import java.util.Date

object SessionizeData {

  val OUTPUT_ARG = 1
  val HTABLE_ARG = 2
  val HFAMILY_ARG = 3
  val CHECKPOINT_DIR_ARG = 4
  val FIXED_ARGS = 5

  val SESSION_TIMEOUT = (60000 * 0.5).toInt

  val TOTAL_SESSION_TIME = "TOTAL_SESSION_TIME"
  val UNDER_A_MINUTE_COUNT = "UNDER_A_MINUTE_COUNT"
  val ONE_TO_TEN_MINUTE_COUNT = "ONE_TO_TEN_MINUTE_COUNT"
  val OVER_TEN_MINUTES_COUNT = "OVER_TEN_MINUTES_COUNT"
  val NEW_SESSION_COUNTS = "NEW_SESSION_COUNTS"
  val TOTAL_SESSION_COUNTS = "TOTAL_SESSION_COUNTS"
  val EVENT_COUNTS = "EVENT_COUNTS"
  val DEAD_SESSION_COUNTS = "DEAD_SESSION_COUNTS"
  val REVISTE_COUNT = "REVISTE_COUNT"
  val TOTAL_SESSION_EVENT_COUNTS = "TOTAL_SESSION_EVENT_COUNTS"

  val dateFormat = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss Z")

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("SessionizeData {sourceType} {outputDir} {source information}")
      println("SessionizeData file {outputDir} {table} {family}  {hdfs checkpoint directory} {source file}")
      println("SessionizeData newFile {outputDir} {table} {family}  {hdfs checkpoint directory} {source file}")
      println("SessionizeData socket {outputDir} {table} {family}  {hdfs checkpoint directory} {host} {port}")
      return ;
    }

    val outputDir = args(OUTPUT_ARG)
    val hTableName = args(HTABLE_ARG)
    val hFamily = args(HFAMILY_ARG)
    val checkpointDir = args(CHECKPOINT_DIR_ARG)

    //This is just creating a Spark Config object.  I don’t do much here but 
    //add the app name.  There are tons of options to put into the Spark config, 
    //but none are needed for this simple example.
    val sparkConf = new SparkConf().
      setAppName("SessionizeData " + args(0)).
      set("spark.cleaner.ttl", "120000")
    
    //These two lines will get us out SparkContext and our StreamingContext.  
    //These objects have all the root functionality we need to get started.
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    
    //Here are are loading our HBase Configuration object.  This will have 
    //all the information needed to connect to our HBase cluster.  
    //There is nothing different here from when you normally interact with HBase.
    //val conf = HBaseConfiguration.create();
    //conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    //conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    
    //This is a HBaseContext object.  This is a nice abstraction that will hide 
    //any complex HBase stuff from us so we can focus on our business case
    //HBaseContext is from the SparkOnHBase project which can be found at
    // https://github.com/tmalaska/SparkOnHBase

    //This is create a reference to our root DStream.  DStreams are like RDDs but 
    //with the context of being in micro batch world.  I set this to null now 
    //because I later give the option of populating this data from HDFS or from 
    //a socket.  There is no reason this could not also be populated by Kafka, 
    //Flume, MQ system, or anything else.  I just focused on these because 
    //there are the easiest to set up.
    var lines: DStream[String] = null

    //Options for data load.  Will be adding Kafka and Flume at some point
    if (args(0).equals("socket")) {
      val host = args(FIXED_ARGS)
      val port = args(FIXED_ARGS + 1)

      println("host:" + host)
      println("port:" + Integer.parseInt(port))

      //Simple example of how you set up a receiver from a Socket Stream
      lines = ssc.socketTextStream(host, port.toInt)
    } else if (args(0).equals("newFile")) {

      val directory = args(FIXED_ARGS)
      println("directory:" + directory)
      
      //Simple example of how you set up a receiver from a HDFS folder
      lines = ssc.fileStream[LongWritable, Text, TextInputFormat](directory, (t: Path) => true, true).map(_._2.toString)
    } else {
      throw new RuntimeException("bad input type")
    }

    val ipKeyLines = lines.map[(String, (Long, Long, String))](eventRecord => {
      //Get the time and ip address out of the original event
      val time = dateFormat.parse(
        eventRecord.substring(eventRecord.indexOf('[') + 1, eventRecord.indexOf(']'))).
        getTime()
      val ipAddress = eventRecord.substring(0, eventRecord.indexOf(' '))
      
      //We are return the time twice because we will use the first at the start time
      //and the second as the end time
      (ipAddress, (time, time, eventRecord))
    })

    val latestSessionInfo = ipKeyLines.
      map[(String, (Long, Long, Long))](a => {
        //transform to (ipAddress, (time, time, counter)) 
        (a._1, (a._2._1, a._2._2, 1))
      }).
      reduceByKey((a, b) => {
        //transform to (ipAddress, (lowestStartTime, MaxFinishTime, sumOfCounter))
        (Math.min(a._1, b._1), Math.max(a._2, b._2), a._3 + b._3)
      }).
      updateStateByKey(updateStatbyOfSessions)

    //remove old sessions
    val onlyActiveSessions = latestSessionInfo.filter(t => System.currentTimeMillis() - t._2._2 < SESSION_TIMEOUT)
    val totals = onlyActiveSessions.mapPartitions[(Long, Long, Long, Long)](it =>
      {
        var totalSessionTime: Long = 0
        var underAMinuteCount: Long = 0
        var oneToTenMinuteCount: Long = 0
        var overTenMinutesCount: Long = 0

        it.foreach(a => {
          val time = a._2._2 - a._2._1
          totalSessionTime += time
          if (time < 60000) underAMinuteCount += 1
          else if (time < 600000) oneToTenMinuteCount += 1
          else overTenMinutesCount += 1
        })

        Iterator((totalSessionTime, underAMinuteCount, oneToTenMinuteCount, overTenMinutesCount))
      }, true).reduce((a, b) => {
        //totalSessionTime, underAMinuteCount, oneToTenMinuteCount, overTenMinutesCount
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
    }).map[HashMap[String, Long]](t => HashMap(
        (TOTAL_SESSION_TIME, t._1), 
        (UNDER_A_MINUTE_COUNT, t._2), 
        (ONE_TO_TEN_MINUTE_COUNT, t._3), 
        (OVER_TEN_MINUTES_COUNT, t._4)))

    val newSessionCount = onlyActiveSessions.filter(t => {
        //is the session newer then that last micro batch
        //and is the boolean saying this is a new session true
        (System.currentTimeMillis() - t._2._2 < 11000 && t._2._4)
      }).
      count.
      map[HashMap[String, Long]](t => HashMap((NEW_SESSION_COUNTS, t)))

    val totalSessionCount = onlyActiveSessions.
      count.
      map[HashMap[String, Long]](t => HashMap((TOTAL_SESSION_COUNTS, t)))

    val totalSessionEventCount = onlyActiveSessions.map(a => a._2._3).reduce((a, b) => a + b).
      count.
      map[HashMap[String, Long]](t => HashMap((TOTAL_SESSION_EVENT_COUNTS, t)))

    val totalEventsCount = ipKeyLines.count.map[HashMap[String, Long]](t => HashMap((EVENT_COUNTS, t)))

    val deadSessionsCount = latestSessionInfo.filter(t => {
      val gapTime = System.currentTimeMillis() - t._2._2
      gapTime > SESSION_TIMEOUT && gapTime < SESSION_TIMEOUT + 11000
    }).count.map[HashMap[String, Long]](t => HashMap((DEAD_SESSION_COUNTS, t)))

    /*
    val allCounts = newSessionCount.
      union(totalSessionCount).
      union(totals).
      union(totalEventsCount).
      union(deadSessionsCount).
      union(totalSessionEventCount).
      reduce((a, b) => b ++ a)

    hbaseContext.streamBulkPut[HashMap[String, Long]](
      allCounts, //The input RDD
      hTableName, //The name of the table we want to put too
      (t) => {
        //Here we are converting our input record into a put
        //The rowKey is C for Count and a backward counting time so the newest 
        //count show up first in HBase's sorted order
        val put = new Put(Bytes.toBytes("C." + (Long.MaxValue - System.currentTimeMillis())))
        //We are iterating through the HashMap to make all the columns with their counts
        t.foreach(kv => put.add(Bytes.toBytes(hFamily), Bytes.toBytes(kv._1), Bytes.toBytes(kv._2.toString)))
        put
      }, 
      false)
      */

    //Persist to HDFS 
    ipKeyLines.join(onlyActiveSessions).
      map(t => {
        //Session root start time | Event message 
        dateFormat.format(new Date(t._2._2._1)) + "\t" + t._2._1._3
      }).
      saveAsTextFiles(outputDir + "/session", "txt")

    ssc.checkpoint(checkpointDir)

    ssc.start
    ssc.awaitTermination
  }

  /**
   * This function will be called for to union of keys in the Reduce DStream 
   * with the active sessions from the last micro batch with the ipAddress 
   * being the key
   * 
   * To goal is that this produces a stateful RDD that has all the active 
   * sessions.  So we add new sessions and remove sessions that have timed 
   * out and extend sessions that are still going
   */
  def updateStatbyOfSessions(
      //(sessionStartTime, sessionFinishTime, countOfEvents)
      a: Seq[(Long, Long, Long)], 
      //(sessionStartTime, sessionFinishTime, countOfEvents, isNewSession)
      b: Option[(Long, Long, Long, Boolean)] 
    ): Option[(Long, Long, Long, Boolean)] = { 
    
    //This function will return a Optional value.  
    //If we want to delete the value we can return a optional "None".  
    //This value contains four parts 
    //(startTime, endTime, countOfEvents, isNewSession)
    var result: Option[(Long, Long, Long, Boolean)] = null

    // These if statements are saying if we didn’t get a new event for 
    //this session’s ip address for longer then the session 
    //timeout + the batch time then it is safe to remove this key value 
    //from the future Stateful DStream
    if (a.size == 0) {
      if (System.currentTimeMillis() - b.get._2 < SESSION_TIMEOUT + 11000) {
        result = None
      } else {
        if (b.get._4 == false) {
          result = b
        } else {
          result = Some((b.get._1, b.get._2, b.get._3, false))
        }
      }
    }

    //Now because we used the reduce function before this function we are 
    //only ever going to get at most one event in the Sequence. 
    a.foreach(c => {
      if (b.isEmpty) {
        //If there was no value in the Stateful DStream then just add it 
        //new, with a true for being a new session
        result = Some((c._1, c._2, c._3, true))
      } else {
        if (c._1 - b.get._2 < SESSION_TIMEOUT) {
          //If the session from the stateful DStream has not timed out 
          //then extend the session
          result = Some((
              Math.min(c._1, b.get._1), //newStartTime 
              Math.max(c._2, b.get._2), //newFinishTime
              b.get._3 + c._3, //newSumOfEvents
              false //This is not a new session
            ))
        } else {
          //Otherwise remove the old session with a new one
          result = Some((
              c._1, //newStartTime
              c._2, //newFinishTime
              b.get._3, //newSumOfEvents
              true //new session
            ))
        }
      }
    })
    result
  }
}