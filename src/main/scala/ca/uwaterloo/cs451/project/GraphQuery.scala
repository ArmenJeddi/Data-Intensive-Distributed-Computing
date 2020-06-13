package ca.uwaterloo.cs451.project


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import ca.uwaterloo.cs451.lib.ShortestPath


class GraphQueryConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, srcnode, agerange, pagerank)
  val input = opt[String](descr = "input path", required = true)
  val srcnode = opt[String](descr = "node to search from", required = true)
//   val interest = opt[String](descr = "technical interests", required = true)
  val agerange = opt[String](descr = "age range", required = false, default = Some("18-75"))
  val pagerank = toggle("pagerank")
  verify()
}

// case class User(val age:Int, val interests: List[String])

object GraphQuery {
    val log = Logger.getLogger(getClass().getName())
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    private val INFINITY = Int.MaxValue.toDouble

    def main(argv: Array[String]): Unit = {
        val startTime = System.nanoTime
        val args = new GraphQueryConf(argv)

        log.info("Input: " + args.input())
        log.info("Source node: " + args.srcnode())
        // log.info("Interests: " + args.interests())
        log.info("Age range: " + args.agerange())
        log.info("Use page rank: " + args.pagerank.isSupplied)

        val conf = new SparkConf().setAppName("Graph Querying System")
        val sc = new SparkContext(conf)

        var vertexPath = args.input() + "/vertices"
        if (args.input().endsWith("/"))
            vertexPath += "vertices"

        var edgePath = args.input() + "/edges"
        if (args.input().endsWith("/"))
            edgePath += "edges"

        val vertexRDD : RDD[(VertexId, User)] = sc.objectFile(vertexPath)
        val edgeRDD : RDD[Edge[Double]] = sc.objectFile(edgePath)

        val graph : Graph[User, Double] = Graph(vertexRDD, edgeRDD, User(-1, List()))

        val spSourceNode: VertexId = args.srcnode().toLong
        val spGraph : Graph[Double, Double] = ShortestPath.run(graph, spSourceNode)

        val sourceInterests : List[String] = graph.vertices.filter(p => p._1 == spSourceNode).first._2.interests

        val joinedGraph = graph.outerJoinVertices(spGraph.vertices) {
            (vid, user, pathLength) => (user, pathLength.getOrElse(INFINITY)) 
        }

        var prPlMain : Graph[(User, Double, Double), Double] = null

        if (args.pagerank.isSupplied){
            val prGraph : Graph[Double, Double] = graph.staticPageRank(20)
            val maxPr = prGraph.vertices.map(p => p._2).max
            val normalizedPrGraph : Graph[Double, Double] = prGraph.mapVertices{
                (vid, pr) => pr / maxPr
            }

            prPlMain = joinedGraph.outerJoinVertices(normalizedPrGraph.vertices){
                (vid, vd, pr) => (vd._1, vd._2, pr.getOrElse(0.0)) 
            }
        }

        var rankGraph: Graph[(User, Double), Double] = null
        
        if (args.pagerank.isSupplied){
            rankGraph = prPlMain.mapVertices{
                case (vid, (user, pl, pr)) => {
                    val intersect = user.interests.intersect(sourceInterests).size.toDouble / sourceInterests.size.toDouble
                    (user, math.exp(pr + intersect - pl))
                }
            }
        } else{
            rankGraph = joinedGraph.mapVertices{
                case (vid, (user, pl)) => {
                    val intersect = user.interests.intersect(sourceInterests).size.toDouble / sourceInterests.size.toDouble
                    (user, math.exp(intersect - pl))
                }
            }
        }

        val ageStr = args.agerange().split("-")
        val startAge = ageStr(0).toInt
        val endAge = ageStr(1).toInt

        // age -> why put filtering here? because if not, and do it by finding subgraph in the start
        val sortedUsers = rankGraph.vertices.filter{ case (vid, (user, rank)) => user.age >= startAge && user.age <= endAge || vid == spSourceNode}
        .map(p => (p._2._2, (p._1, p._2._1)))
        .sortByKey(false)
        .take(50)

        val outputDir = new Path("rank-temp")
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        sc.parallelize(sortedUsers)
        .map{case (rank, (vid, user)) =>{ 
            var interests = user.interests(0)
            for(i <- 1 to user.interests.size-1) interests += "-" + user.interests(i)
            (vid, rank, user.age, interests)
        }}
        .coalesce(1)
        .saveAsTextFile("rank-temp")
        val duration = (System.nanoTime - startTime) / 1e9d
        println(s"\n\nprogram duration: $duration seconds")
    }

} 
