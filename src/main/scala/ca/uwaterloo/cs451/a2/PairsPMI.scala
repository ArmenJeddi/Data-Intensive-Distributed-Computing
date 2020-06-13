

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap
import java.io.{BufferedInputStream, OutputStreamWriter}
import org.apache.hadoop.conf.Configuration

class PMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "PMI threshold", required = false, default = Some(1))
  verify()
}

// class BigramPartitioner(numberOfPartitioner: Int) extends Partitioner{
//     override def numPartitions: Int = numberOfPartitioner

//     override def getPartition(key: Any): Int = {
//         val pair = key.asInstanceOf[(String, String)]
//         Math.abs(pair._1.hashCode()% numPartitions)
//     }

//     override def equals(other: Any): Boolean = other match {
//         case partitioner: BigramPartitioner =>
//         partitioner.numPartitions == numPartitions
//         case _ => 
//         false
//     }

// }

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  val line_limit : Int = 40

//   val conf = new SparkConf().setAppName("Pairs PMI")
//   val sc : SparkContext


  def main(argv: Array[String]) {
    val args = new PMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("PMI threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val broadcastThreshold = sc.broadcast(args.threshold())

    countLines(args.input(), sc, args.reducers())

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val counts = textFile.flatMap(line => {
        val tokens = tokenize(line)
        
        val line_counts = scala.collection.mutable.ListBuffer[(String, String)]()

        if (tokens.length > 1){
            for (i <- 0 to Math.min(tokens.length, line_limit)-1){
                for (j <- 0 to Math.min(tokens.length, line_limit)-1){
                    if (i!=j){
                        if (!line_counts.contains((tokens(i), tokens(j)))){
                            line_counts += ((tokens(i), tokens(j)))
                        }
                    }
                }
            }
         }
         line_counts.toList
      })
      .map(bigram => (bigram, 1))

      // repartition with partitioner?? --> kinda combine them before mapping?

      .reduceByKey(_ + _)

      .mapPartitions(part => { 
            val hdfs = FileSystem.get(new Configuration())
            val is = new BufferedInputStream( hdfs.open( new Path( "./l1/part-00000" ) ) )
            val unaries = HashMap[String, Float]()
            val lines = scala.io.Source.fromInputStream( is ).getLines()

            lines.foreach{ case v => {
                val word = v.substring(0, v.indexOf(' '))
                val count = v.substring(v.indexOf(' ') + 1)
                unaries.put(word, count.toFloat)
              }
            }
            
            part.map(p => {
                val a_tot = unaries.getOrElse(p._1._1, 0.0f)
                val b_tot = unaries.getOrElse(p._1._2, 0.0f)
                val lines_tot = unaries.getOrElse("*Total-Count", 0.0f)

                val PMI = (p._2.toFloat * lines_tot ) / (a_tot * b_tot)

                (p._1, (Math.log10(PMI),p._2)  )
            
            })
            .filter(p => p._2._2 >= broadcastThreshold.value)

        })
      


    counts.saveAsTextFile(args.output())
    println("\n\n")
    
  }


  def countLines(io_inp: String, sc: SparkContext, parts: Int) {
    val io_out = "l1"
    val outputDir = new Path(io_out)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(io_inp, parts)
    val counts = textFile.flatMap(line => {
        val tokens = tokenize(line)
        
        val line_counts = scala.collection.mutable.ListBuffer[String]("*Total-Count")

        if (tokens.length > 0){
            for (i <- 0 to Math.min(tokens.length, line_limit)-1){
                if (!line_counts.contains(tokens(i)))
                    line_counts += tokens(i)
            }
         }
         line_counts.toList
      })
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .map(p => p._1 + " " + p._2)

    counts.coalesce(1).saveAsTextFile(io_out)

  }

}
