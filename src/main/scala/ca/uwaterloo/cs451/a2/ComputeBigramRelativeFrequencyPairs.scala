

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class BigramPartitioner(numberOfPartitioner: Int) extends Partitioner{
    override def numPartitions: Int = numberOfPartitioner

    override def getPartition(key: Any): Int = {
        val pair = key.asInstanceOf[(String, String)]
        Math.abs(pair._1.hashCode()% numPartitions)
    }

    override def equals(other: Any): Boolean = other match {
        case partitioner: BigramPartitioner =>
        partitioner.numPartitions == numPartitions
        case _ =>
        false
    }

}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram RF Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val counts = textFile.flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p=> List( (p(0), p(1)), (p(0), "*"))).flatten else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new BigramPartitioner(args.reducers()))
      .mapPartitions(part => {
        var marginal = 0.0f
        part.map(p => {
            if (p._1._2.equals("*")){
                marginal = p._2.toFloat
                (p._1, p._2.toFloat)
            }
            else{
                val relativity = p._2.toFloat / marginal
                (p._1, relativity) 
            }
        })
      })


    counts.saveAsTextFile(args.output())
    println("\n\n")
    
  }
}