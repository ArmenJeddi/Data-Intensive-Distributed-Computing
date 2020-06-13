package ca.uwaterloo.cs451.a5

// import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
// import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Q1Conf(input:String, date:String, readType:String)

object Q1 {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(argv: Array[String]) {
    
    if (argv.length != 5){
        println("incorrect arguments")
        return
    }
    else if(!argv(0).equals("--input") || !argv(2).equals("--date") || (!argv(4).equals("--text")  && !argv(4).equals("--parquet"))){
        println("incorrect arguments")
        return
    }
    
    var input_type = "text"
    if (argv(4).equals("--parquet")){
        input_type = "parquet"
    }

    var input_path = argv(1)
    if (!input_path.contains("data")){
        input_path = "data/" + input_path
    }

    val config = Q1Conf(input_path, argv(3), input_type)

    log.info("Input: " + config.input)
    log.info("date: " + config.date)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val broadcastDate = sc.broadcast(config.date)

    if (config.readType.equals("text")){
        val l_item_table = sc.textFile(config.input + "/lineitem.tbl")

        val counts = l_item_table.map(l => {
            val arr = l.split('|')
            arr(10)
        })
        .filter(p => p.equals(broadcastDate.value)).count
        println("ANSWER=" + counts)
    }
    else{
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(config.input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val counts = lineitemRDD.filter(l => l(10).equals(broadcastDate.value)).count
        println("ANSWER=" + counts)
    }

    // val outputDir = new Path(args.output())
    // FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

  }

}
