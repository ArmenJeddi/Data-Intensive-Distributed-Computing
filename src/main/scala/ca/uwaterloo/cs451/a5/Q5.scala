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

case class Q5Conf(input:String, readType:String)

object Q5 {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]) {
    
    if (argv.length != 3){
        println("incorrect arguments")
        return
    }
    else if(!argv(0).equals("--input") || (!argv(2).equals("--text")  && !argv(2).equals("--parquet"))){
        println("incorrect arguments")
        return
    }
    
    var input_type = "text"
    if (argv(2).equals("--parquet")){
        input_type = "parquet"
    }

    var input_path = argv(1)
    if (!input_path.contains("data")){
        input_path = "data/" + input_path
    }

    val config = Q5Conf(input_path, input_type)

    log.info("Input: " + config.input)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

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


        val plot_data = l_item_table.map(l => {
                val arr = l.split('|')
                (arr(0), arr(10).substring(0,7) )
            }
        )
        .cogroup(order_table.map( o => {
                    val arr = o.split('|')
                    (arr(0), arr(1))
                }
            ).filter(o => broadcastCustomers.value.contains(o._2))
            .map(o => (o._1, broadcastCustomers.value(o._2)))
        )
        .filter(p =>  p._2._1.size > 0)
        .flatMap(p => {for (i<- p._2._1) yield (p._1, (i, p._2._2))})
        .filter(p => {
                if (!broadcastNations.value.contains(p._2._2.head)){
                    false
                } else {
                    val p_nation = broadcastNations.value(p._2._2.head)
                    if (p_nation.equals("CANADA") || p_nation.equals("UNITED STATES") ){
                        true
                    }
                    else{
                        false
                    }
                }
            } 
        )
        .map(p => ((p._2._1, p._2._2.head), 1) )
        .reduceByKey(_ + _)
        .map(p => ((p._1._1.substring(0,4).toInt, p._1._1.substring(5,7).toInt), (p._1._2, p._2 )) )
        .sortByKey()
        .map(p => {
                var dateStr = p._1._1.toString + "-"
                if (p._1._2 < 10){
                    dateStr = dateStr + "0" + p._1._2.toString
                }else{
                    dateStr = dateStr + p._1._2.toString
                }
                (dateStr, broadcastNations.value(p._2._1), p._2._2)
            }
        )
        .collect()
        
        // Canada
        println("Canada: ")
        plot_data.foreach(p => {
            if (p._2.equals("CANADA")){
                println("(" + p._1 + "," + p._2 + "," + p._3 + ")")
            }
        })

        // US
        println("USA: ")
        plot_data.foreach(p => {
            if (p._2.equals("UNITED STATES")){
                println("(" + p._1 + "," + p._2 + "," + p._3 + ")")
            }
        })

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

        val plot_data = lineitemRDD.map(l => (l(0).toString, l(10).toString.substring(0, 7)))
        .cogroup(orderRDD.map( o => (o(0).toString, o(1).toString) )
            .filter(o => broadcastCustomers.value.contains(o._2))
            .map(o => (o._1, broadcastCustomers.value(o._2)))
        )
        .filter(p =>  p._2._1.size > 0)
        .flatMap(p => {for (i<- p._2._1) yield (p._1, (i, p._2._2))})
        .filter(p => {
                if (!broadcastNations.value.contains(p._2._2.head)){
                    false
                }else{
                    val p_nation = broadcastNations.value(p._2._2.head)
                    if (p_nation.equals("CANADA") || p_nation.equals("UNITED STATES") ){
                        true
                    }
                    else{
                        false
                    }
                }
            } 
        )
        .map(p => ((p._2._1, p._2._2.head), 1) )
        .reduceByKey(_ + _)
        .map(p => ((p._1._1.substring(0,4).toInt, p._1._1.substring(5,7).toInt), (p._1._2, p._2 )))
        .sortByKey()
        .map(p => {
                var dateStr = p._1._1.toString + "-"
                if (p._1._2 < 10){
                    dateStr = dateStr + "0" + p._1._2.toString
                }else{
                    dateStr = dateStr + p._1._2.toString
                }
                (dateStr, broadcastNations.value(p._2._1), p._2._2)
            }
        )
        .collect()
        
        // Canada
        println("Canada: ")
        plot_data.foreach(p => {
            if (p._2.equals("CANADA")){
                println("(" + p._1 + "," + p._2 + "," + p._3 + ")")
            }
        })

        // US
        println("USA: ")
        plot_data.foreach(p => {
            if (p._2.equals("UNITED STATES")){
                println("(" + p._1 + "," + p._2 + "," + p._3 + ")")
            }
        })

    }
  }

}
