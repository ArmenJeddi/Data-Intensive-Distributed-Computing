package ca.uwaterloo.cs451.a6

// import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import scala.collection.mutable.Map

import org.apache.log4j.Logger
import org.apache.log4j.Level

class ApplyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]) {

    val args = new ApplyConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model path: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val testData = sc.textFile(args.input())

    val model = sc.textFile(args.model()+"/part-00000")
    .map(l => {
    	val pair = l.substring(1, l.length-1).split(",")
    	(pair(0).toInt, pair(1).toDouble)
	})
    .collectAsMap
    val broadcastModel = sc.broadcast(model)

    // preds
    testData.map(l =>{
        val arr = l.split(" ")
        val features = arr.drop(2).map(e => e.toInt)
        var total_score = 0d
        features.foreach(f => {
            if (broadcastModel.value.contains(f))
                total_score += broadcastModel.value(f.toInt)
            
        })
        val pred = if (total_score > 0) "spam" else "ham"
        (arr(0), arr(1), total_score, pred)
    })
    .saveAsTextFile(args.output())

  }

}