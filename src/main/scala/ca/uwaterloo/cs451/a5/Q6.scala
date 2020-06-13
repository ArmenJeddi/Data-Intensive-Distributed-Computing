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

case class Q6Conf(input:String, date:String, readType:String)

object Q6 {
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

    val config = Q6Conf(input_path, argv(3), input_type)

    log.info("Input: " + config.input)
    log.info("date: " + config.date)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val broadcastDate = sc.broadcast(config.date)

    if (config.readType.equals("text")){
        val l_item_table = sc.textFile(config.input + "/lineitem.tbl")

        l_item_table.map(l => {
            val arr = l.split('|')
            (arr(8), arr(9), arr(4), arr(5), arr(6), arr(7), arr(10) )   // order: l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate
        })
        .filter(p => p._7.equals(broadcastDate.value))
        .map(p => {
            val l_quantity = p._3.toDouble
            val l_extendedprice = p._4.toDouble
            val l_discount = p._5.toDouble
            val l_tax = p._6.toDouble
            val disc_price = l_extendedprice * (1-l_discount)
            val charge = disc_price * (1+l_tax)
            ((p._1, p._2), (l_quantity, l_extendedprice, l_discount, disc_price, charge, 1))
        })
        .reduceByKey{ case (x,y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6)}
        .map(p => (p._1._1, p._1._2, p._2._1, p._2._2, p._2._4, p._2._5, p._2._1/p._2._6, p._2._2/p._2._6, p._2._3/p._2._6, p._2._6))
        .collect()
        .foreach(println)
    }
    else{
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(config.input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        
        lineitemRDD.map(l => (l(8).toString, l(9).toString, l(4).toString, l(5).toString, l(6).toString, l(7).toString, l(10).toString) )
        .filter(p => p._7.equals(broadcastDate.value))
        .map(p => {
            val l_quantity = p._3.toDouble
            val l_extendedprice = p._4.toDouble
            val l_discount = p._5.toDouble
            val l_tax = p._6.toDouble
            val disc_price = l_extendedprice * (1-l_discount)
            val charge = disc_price * (1+l_tax)
            ((p._1, p._2), (l_quantity, l_extendedprice, l_discount, disc_price, charge, 1))
        })
        // .reduceByKey(lambda x,y: (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6))
        .reduceByKey{ case (x,y) => (x._1+y._1, x._2+y._2, x._3+y._3, x._4+y._4, x._5+y._5, x._6+y._6)}
        .map(p=> (p._1._1, p._1._2, p._2._1, p._2._2, p._2._4, p._2._5, p._2._1/p._2._6, p._2._2/p._2._6, p._2._3/p._2._6, p._2._6))
        .collect()
        .foreach(println)
    }


    // val outputDir = new Path(args.output())
    // FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

  }

}
