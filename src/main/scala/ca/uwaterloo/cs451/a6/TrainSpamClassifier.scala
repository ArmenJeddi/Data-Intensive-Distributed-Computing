package ca.uwaterloo.cs451.a6

// import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import scala.collection.mutable.Map
import scala.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

class SpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output path", required = true)
  val shuffle = toggle("shuffle")
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]) {

    val args = new SpamConf(argv)

    log.info("Input: " + args.input())
    log.info("Out path: " + args.model())
    log.info("Shuffle: " + args.shuffle.isSupplied)

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // from assignment file
    val w = Map[Int, Double]()
    val delta = 0.002
    
    def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
    }

    var trainData = sc.textFile(args.input())
    if (args.shuffle.isSupplied){
        trainData = sc.textFile(args.input())
        .map(l =>{
            (Random.nextDouble(), l)
        })
        .sortByKey()
        .map(l => l._2)
    }

    trainData.map( l => {
        val arr = l.split(" ")
        val isSpam = if (arr(1).equals("spam")) 1 else 0
        val features = arr.drop(2).map(e => e.toInt)
        (0, (arr(0), isSpam, features) )
    })
    .groupByKey(1)
    .flatMap(p => {
        p._2.foreach( v => {
            val isSpam = v._2
            val features = v._3
            val score = spamminess(features)
            val prob = 1.0 / (1 + Math.exp(-score))
            
            features.foreach(f => {
                if (w.contains(f)) {
                    w(f) += (isSpam - prob) * delta
                } else {
                    w(f) = (isSpam - prob) * delta
                }
            })
            
        })
        w
    })
    .saveAsTextFile(args.model())
  }

}