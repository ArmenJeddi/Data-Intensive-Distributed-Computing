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

case class Q3Conf(input:String, date:String, readType:String)

object Q3 {
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

    val config = Q3Conf(input_path, argv(3), input_type)

    log.info("Input: " + config.input)
    log.info("date: " + config.date)
    log.info("input type: " + config.readType)

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val broadcastDate = sc.broadcast(config.date)

    if (config.readType.equals("text")){
        val l_item_table = sc.textFile(config.input + "/lineitem.tbl")
        val part_table = sc.textFile(config.input + "/part.tbl")
        val supplier_table = sc.textFile(config.input + "/supplier.tbl")

        val parts = part_table.map(p => {
            val arr = p.split('|')
            (arr(0), arr(1))
        }).collectAsMap()
        val broadcastParts = sc.broadcast(parts)

        val suppliers = supplier_table.map(s =>{
            val arr = s.split('|')
            (arr(0), arr(1))
        }).collectAsMap()
        val broadcastSuppliers = sc.broadcast(suppliers)

        l_item_table.map(l => {
                val arr = l.split('|')
                (arr(0), arr(1), arr(2), arr(10))
            }
        ).filter(p => p._4.equals(broadcastDate.value))
        .filter(p => broadcastParts.value.contains(p._2) && broadcastSuppliers.value.contains(p._3))
        .map(p => (p._1, (broadcastParts.value(p._2), broadcastSuppliers.value(p._3)) ) )
        .map(p => (p._1.toInt, p._2))
        .sortByKey()
        .take(20)
        .map(p => (p._1, p._2._1, p._2._2))
        .foreach(println)

    }
    else{
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(config.input + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val partDF = sparkSession.read.parquet(config.input + "/part")
        val partRDD = partDF.rdd
        val supplierDF = sparkSession.read.parquet(config.input + "/supplier")
        val supplierRDD = supplierDF.rdd

        val parts = partRDD.map(p => (p(0).toString, p(1).toString)).collectAsMap()
        val broadcastParts = sc.broadcast(parts)

        val suppliers = supplierRDD.map(s => (s(0).toString, s(1).toString)).collectAsMap()
        val broadcastSuppliers = sc.broadcast(suppliers)

        lineitemRDD.map(l => (l(0).toString, l(1).toString, l(2).toString, l(10).toString ))
        .filter(p => p._4.equals(broadcastDate.value))
        .filter(p => broadcastParts.value.contains(p._2) && broadcastSuppliers.value.contains(p._3))
        .map(p => (p._1, (broadcastParts.value(p._2), broadcastSuppliers.value(p._3)) ) )
        .map(p => (p._1.toInt, p._2))
        .sortByKey()
        .take(20)
        .map(p => (p._1, p._2._1, p._2._2))
        .foreach(println)
    }
  }

}
