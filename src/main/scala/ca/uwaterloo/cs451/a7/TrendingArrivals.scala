package ca.uwaterloo.cs451.a7 

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{StateSpec, State}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable
import java.text.DecimalFormat

import org.apache.log4j.Logger
import org.apache.log4j.Level

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()


    val goldman = List((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753))
    val gold_max_x = goldman.map(_._1).max
    val gold_min_x = goldman.map(_._1).min 
    val gold_max_y = goldman.map(_._2).max
    val gold_min_y = goldman.map(_._2).min

    val citigroup = List((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267))
    val citi_max_x = citigroup.map(_._1).max
    val citi_min_x = citigroup.map(_._1).min 
    val citi_max_y = citigroup.map(_._2).max
    val citi_min_y = citigroup.map(_._2).min 


    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val state = StateSpec.function(
        (key: String, current: Option[Int], state: State[Int]) => {
        
        
        (current, state.getOption) match {
            case (Some(x), Some(prev)) => {
                val prevState = prev
                state.update(x)
                (key, prevState, x)
            }
            case (Some(x), None) => {
                state.update(x)
                (key, 0, x)
            }
            case (None, None) => {
                state.update(-1)
                (key, 0, 0)
            }
            case _ => (key, 0, 0)
        }
        }
    )

    val wc = stream.map(p =>{
        val arr = p.split(",")
        val taxi_type = arr(0)
        var longitude = arr(8)
        var latitude = arr(9)
        if (taxi_type.equals("yellow")){
            longitude = arr(10)
            latitude = arr(11)
        }

        val d_longitude = longitude.toDouble
        val d_latitude = latitude.toDouble

        var drop_loc = "unknown"
        if ((d_longitude > gold_min_x) && (d_longitude < gold_max_x) && (d_latitude > gold_min_y) && (d_latitude < gold_max_y)){
            drop_loc = "goldman"
        }
        if ((d_longitude > citi_min_x) && (d_longitude < citi_max_x) && (d_latitude > citi_min_y) && (d_latitude < citi_max_y)){
            drop_loc = "citigroup"
        }

        (drop_loc, 1)
    })
    .filter(p => !p._1.equals("unknown"))
    .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
    .mapWithState(state)
    .persist()
    
    val formatter = new DecimalFormat("00000000")
    val directory = args.output() + "/part-"
    wc.foreachRDD((rdd, time) => {
        // println("**********************************************************************************************")
        val timeStr = formatter.format(time.milliseconds)
        rdd.map(record => (record._1, (record._3, time.milliseconds, record._2))).coalesce(1).saveAsTextFile(directory  + timeStr)
        rdd.foreach(record => {
            val location = if (record._1.equals("goldman")) "Goldman Sachs" else "Citigroup"
            if (record._2 > 0 && record._3 >= 10 && record._3 >= 2* record._2)
                println(s"Number of arrivals to $location has doubled from ${record._2} to ${record._3} at ${time.milliseconds}!")
        })
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}