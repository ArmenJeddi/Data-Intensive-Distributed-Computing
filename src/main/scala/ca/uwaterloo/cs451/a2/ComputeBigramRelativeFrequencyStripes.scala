

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.immutable.Map

// class Conf(args: Seq[String]) extends ScallopConf(args) {
//   mainOptions = Seq(input, output, reducers)
//   val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   verify()
// }

class BigramPartitionerStripes(numberOfPartitioner: Int) extends Partitioner{
    override def numPartitions: Int = numberOfPartitioner

    override def getPartition(key: Any): Int = {
        val key1 = key.asInstanceOf[String]
        Math.abs(key1.hashCode()% numPartitions)
    }

    override def equals(other: Any): Boolean = other match {
        case partitioner: BigramPartitionerStripes =>
        partitioner.numPartitions == numPartitions
        case _ =>
        false
    }

}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram RF Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val counts = textFile

      .flatMap(line => {

        val tokens = tokenize(line)

        if (tokens.length > 1) tokens.sliding(2).toList.map(p => {
          val stripe = Map[String,Int](p(1) -> 1)
          (p(0), stripe ) 
        })
        else List()
   
      })

      .reduceByKey((v1, v2) => v1 ++ v2.map{ case (k,v) => k -> (v + v1.getOrElse(k, 0)) }  )
      
      .partitionBy(new BigramPartitionerStripes(args.reducers()))
      .mapPartitions(part => {
        
        part.map(p => {
            var sum = 0.0f
            p._2.foreach{case (k,v) => sum += v}
            val relatives = p._2.map{case (k,v) => (k,v/sum)}
            (p._1, relatives)
        })
      })


    counts.saveAsTextFile(args.output())
    println("\n\n")
    
  }
}