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

case class Q4Conf(input:String, date:String, readType:String)

object Q4 {
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

    val config = Q4Conf(input_path, argv(3), input_type)

    log.info("Input: " + config.input)
    log.info("date: " + config.date)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val broadcastDate = sc.broadcast(config.date)

    if (config.readType.equals("text")){
        val l_item_table = sc.textFile(config.input + "/lineitem.tbl")
        val order_table = sc.textFile(config.input + "/orders.tbl")

        val nation_table = sc.textFile(config.input + "/nation.tbl")
        val customer_table = sc.textFile(config.input + "/customer.tbl")

        val nations = nation_table.map(p => {
            val arr = p.split('|')
            (arr(0), arr(1))
        }).collectAsMap()
        val broadcastNations = sc.broadcast(nations)

        val customers = customer_table.map(p => {
            val arr = p.split('|')
            (arr(0), arr(3))
        }).collectAsMap()
        val broadcastCustomers = sc.broadcast(customers)


        l_item_table.map(l => {
                val arr = l.split('|')
                (arr(0), arr(10))
            }
        ).filter(p => p._2.equals(broadcastDate.value))
        .cogroup(order_table.map( o => {
                    val arr = o.split('|')
                    (arr(0), arr(1))
                }
            ).filter(o => broadcastCustomers.value.contains(o._2))
            .map(o => (o._1, broadcastCustomers.value(o._2)))
        )
        .filter(p =>  p._2._1.size > 0)
        .flatMap(p => {for (i<- p._2._1) yield (p._1, (i, p._2._2))})
        .filter(p => broadcastNations.value.contains(p._2._2.head))
        .map(p => ((p._2._2.head.toInt, broadcastNations.value(p._2._2.head)), 1) )
        .reduceByKey(_ + _)
        .sortByKey()
        .map(p => (p._1._1, p._1._2, p._2))
        .collect()
        .foreach(println)

    }
    else{
        val sparkSession = SparkSession.builder.getOrCreate

        val lineitemDF = sparkSession.read.parquet(config.input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val orderDF = sparkSession.read.parquet(config.input + "/orders")
        val orderRDD = orderDF.rdd

        val nationDF = sparkSession.read.parquet(config.input + "/nation")
        val nationRDD = nationDF.rdd
        val nations = nationRDD.map(n => (n(0).toString, n(1).toString)).collectAsMap()
        val broadcastNations = sc.broadcast(nations)

        val customerDF = sparkSession.read.parquet(config.input + "/customer")
        val customerRDD = customerDF.rdd
        val customers = customerRDD.map(c => (c(0).toString, c(3).toString)).collectAsMap()
        val broadcastCustomers = sc.broadcast(customers)

        lineitemRDD.map(l => (l(0).toString, l(10).toString))
        .filter(p => p._2.equals(broadcastDate.value))
        .cogroup(orderRDD.map( o => (o(0).toString, o(1).toString) )
            .filter(o => broadcastCustomers.value.contains(o._2))
            .map(o => (o._1, broadcastCustomers.value(o._2)))
        )
        .filter(p =>  p._2._1.size > 0)
        .flatMap(p => {for (i<- p._2._1) yield (p._1, (i, p._2._2))})
        .filter(p => broadcastNations.value.contains(p._2._2.head))
        .map(p => ((p._2._2.head.toInt, broadcastNations.value(p._2._2.head)), 1) )
        .reduceByKey(_ + _)
        .sortByKey()
        .map(p => (p._1._1, p._1._2, p._2))
        .collect()
        .foreach(println)

    }
  }

}
