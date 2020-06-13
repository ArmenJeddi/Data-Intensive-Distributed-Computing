package ca.uwaterloo.cs451.project

// import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.util.Try

import org.apache.log4j.Logger
import org.apache.log4j.Level

case class User(val age:Int, val interests: List[String])

class BuildConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, weighted, undirected)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val weighted = toggle("weighted")
  val undirected = toggle("undirected")
  verify()
}

object BuildGraph {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]) {
    val startTime = System.nanoTime
    val args = new BuildConf(argv)

    log.info("Input: " + args.input())
    log.info("Out path: " + args.output())
    log.info("Weighted graph: " + args.weighted.isSupplied)
    log.info("Undirected graph: " + args.undirected.isSupplied)

    val conf = new SparkConf().setAppName("Graph Builder!")
    val sc = new SparkContext(conf)

    val bcUnDirected = sc.broadcast(args.weighted.isSupplied)
    val bcWeighted = sc.broadcast(args.undirected.isSupplied)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val fields = List("ai", "bigdata", "ml", "nlp", "vision", "signal", "database", "hci", "algorithm", "optimization", "security", "crypto", "automata", "software", "networks")

    val file = sc.textFile(args.input())

    val vids = file.flatMap(p => p.split("\t").map(_.toLong)).distinct()

    val vertexRDD = vids.map(p => {
        val numInterests = Random.nextInt(4) + 2 // can have anywhere between 2 to 5 interests
        val interests = Random.shuffle(fields).take(numInterests)
        val age = Random.nextInt(58) + 18 // age be anywhere between 18 and 75 inclusive
        (p, User(age, interests))
    })

    val edgeRDD = file.flatMap { triplet =>
      val parts = triplet.split("\t")
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }
      
      val (src, dst) = (parts.head.toLong, parts(1).toLong)
      if (true) {
        Array(Edge(src, dst, weight), Edge(dst, src, weight))
      } else {
        Array(Edge(src, dst, weight))
      }
    }

    val graph : Graph[User, Double] = Graph(vertexRDD, edgeRDD, User(-1, List()))

    var vertexPath = args.output() + "/vertices"
    if (args.output().endsWith("/"))
        vertexPath += "vertices"

    var edgePath = args.output() + "/edges"
    if (args.output().endsWith("/"))
        edgePath += "edges"

    vertexRDD.saveAsObjectFile(vertexPath)
    // val vertexRDD : RDD[(VertexId, User)] = sc.objectFile("location/vertices")

    edgeRDD.saveAsObjectFile(edgePath)
    // val edgeRDD : RDD[Edge[Int]] = sc.objectFile("location/edges")
    val duration = (System.nanoTime - startTime) / 1e9d
    println(s"\n\nprogram duration: $duration seconds")
  }

}
