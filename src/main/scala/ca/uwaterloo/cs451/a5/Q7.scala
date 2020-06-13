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



case class Q7Conf(input:String, date:String, readType:String)

object Q7 {
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

    val config = Q7Conf(input_path, argv(3), input_type)

    log.info("Input: " + config.input)
    log.info("date: " + config.date)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val date_conf = config.date.substring(0, 4) + config.date.substring(5, 7) + config.date.substring(8)

    val broadcastDate = sc.broadcast(date_conf.toInt)

    if (config.readType.equals("text")){
        val l_item_table = sc.textFile(config.input + "/lineitem.tbl")
        val order_table = sc.textFile(config.input + "/orders.tbl")

        val customer_table = sc.textFile(config.input + "/customer.tbl")
        val customers = customer_table.map(p => {
            val arr = p.split('|')
            (arr(0), arr(1))
        }).collectAsMap()
        val broadcastCustomers = sc.broadcast(customers)

        l_item_table.map(l => {
            val arr = l.split('|')
            val l_extendedprice = arr(5).toDouble
            val l_discount = arr(6).toDouble
            val revenue = l_extendedprice*(1-l_discount)
            val date = arr(10).substring(0, 4) + arr(10).substring(5, 7) + arr(10).substring(8)
            (arr(0), ( date.toInt, revenue) )  // order: l_orderkey, shipdate, revenue
        }).filter(p => p._2._1 > broadcastDate.value)
        .cogroup(order_table.map( o => {
                    val arr = o.split('|')
                    val o_date = arr(4).substring(0, 4) + arr(4).substring(5, 7) + arr(4).substring(8)
                    (arr(0), o_date.toInt , arr(1), arr(7)) // o_orderkey o_orderdate, o_custkey, o_shippriority
                }
            )
            .filter(p => p._2 < broadcastDate.value)
            .filter(o => broadcastCustomers.value.contains(o._3))
            .map(o => (o._1, (o._2, broadcastCustomers.value(o._3), o._4)) ) // o_orderkey, o_orderdate, c_name, o_shippriority
        )
        .filter(p =>  p._2._1.size > 0 && p._2._2.size > 0)
        .flatMap(p => {
            val info2 = p._2._2.head
            val c_name = info2._2
            val o_shippriority = info2._3
            val orderStr = info2._1.toString
            val o_orderdate = orderStr.substring(0,4) + "-" + orderStr.substring(4,6) + "-" + orderStr.substring(6,8)
            for (i<- p._2._1) yield ((c_name, p._1, o_orderdate, o_shippriority), i._2)
            
        })
        .reduceByKey(_ + _)
        .map(p => (p._2, p._1))
        .sortByKey(false)
        .take(10)
        .map(p => (p._2._1, p._2._2, p._1, p._2._3, p._2._4))
        .foreach(println)
    }
    else{
        val sparkSession = SparkSession.builder.getOrCreate

        val lineitemDF = sparkSession.read.parquet(config.input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val orderDF = sparkSession.read.parquet(config.input + "/orders")
        val orderRDD = orderDF.rdd

        val customerDF = sparkSession.read.parquet(config.input + "/customer")
        val customerRDD = customerDF.rdd
        val customers = customerRDD.map(c => (c(0).toString, c(1).toString)).collectAsMap()
        val broadcastCustomers = sc.broadcast(customers)


        lineitemRDD.map(l => {
            val l_extendedprice = l(5).toString.toDouble
            val l_discount = l(6).toString.toDouble
            val revenue = l_extendedprice*(1-l_discount)
            val dateStr = l(10).toString
            val date = dateStr.substring(0, 4) + dateStr.substring(5, 7) + dateStr.substring(8)
            (l(0).toString, ( date.toInt, revenue) )  // order: l_orderkey, shipdate, revenue
        })
        .filter(p => p._2._1 > broadcastDate.value)
        .cogroup(orderRDD.map( o => {
                    val dateStr = o(4).toString
                    val o_date = dateStr.substring(0, 4) + dateStr.substring(5, 7) + dateStr.substring(8)
                    (o(0).toString, o_date.toInt , o(1).toString, o(7).toString) // o_orderkey o_orderdate, o_custkey, o_shippriority
                }
            )
            .filter(p => p._2 < broadcastDate.value)
            .filter(o => broadcastCustomers.value.contains(o._3))
            .map(o => (o._1, (o._2, broadcastCustomers.value(o._3), o._4)) ) // o_orderkey, o_orderdate, c_name, o_shippriority
        )
        .filter(p =>  p._2._1.size > 0 && p._2._2.size > 0)
        .flatMap(p => {
            val info2 = p._2._2.head
            val c_name = info2._2
            val o_shippriority = info2._3
            val orderStr = info2._1.toString
            val o_orderdate = orderStr.substring(0,4) + "-" + orderStr.substring(4,6) + "-" + orderStr.substring(6,8)
            for (i<- p._2._1) yield ((c_name, p._1, o_orderdate, o_shippriority), i._2)
            
        })
        .reduceByKey(_ + _)
        .map(p => (p._2, p._1))
        .sortByKey(false)
        .take(10)
        .map(p => (p._2._1, p._2._2, p._1, p._2._3, p._2._4))
        .foreach(println)

    }
  }

}
