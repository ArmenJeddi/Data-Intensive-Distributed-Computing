package ca.uwaterloo.cs451.lib


import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
// import org.apache.spark.Logging

// import org.apache.log4j.Logger
// import org.apache.log4j.Level


object ShortestPath extends Serializable {
//   val log = Logger.getLogger(getClass().getName())
//   Logger.getLogger("org").setLevel(Level.OFF)
//   Logger.getLogger("akka").setLevel(Level.OFF)

  private val INFINITY = Int.MaxValue.toDouble

  private def incrementPath(visitedPath: Double, edgeWeight: Double): Double = {
      visitedPath + edgeWeight
  }

  private def addPaths(path1: Double, path2: Double): Double = {
      math.min(path1, path2)
  }

  def run[VD: ClassTag](
    graph: Graph[VD, Double], sourceNode: VertexId): Graph[Double, Double] = {
        val spGraph = graph.mapVertices { (vid, attr) =>
            if (vid == sourceNode) 0.0 else INFINITY
        }

        val initialMessage = INFINITY

        def vertexProgram(id: VertexId, attr: Double, msg: Double): Double = {
            addPaths(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
            val newAttr = incrementPath(edge.srcAttr, edge.attr)
            if (edge.dstAttr != addPaths(newAttr, edge.dstAttr)) Iterator((edge.dstId, newAttr))
            else Iterator.empty
        }

        Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addPaths)
  }

}
