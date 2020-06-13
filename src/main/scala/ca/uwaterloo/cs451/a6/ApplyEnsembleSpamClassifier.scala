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

class EnsembleConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val method = opt[String](descr = "ensemple method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(argv: Array[String]) {

    val args = new EnsembleConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model path: " + args.model())
    log.info("Ensemble method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // data
    val testData = sc.textFile(args.input())

    val model1 = sc.textFile(args.model()+"/part-00000")
    .map(l => {
    	val pair = l.substring(1, l.length-1).split(",")
    	(pair(0).toInt, pair(1).toDouble)
	})
    .collectAsMap
    val broadcastModel1 = sc.broadcast(model1)

    val model2 = sc.textFile(args.model()+"/part-00001")
    .map(l => {
    	val pair = l.substring(1, l.length-1).split(",")
    	(pair(0).toInt, pair(1).toDouble)
	})
    .collectAsMap
    val broadcastModel2 = sc.broadcast(model2)

    val model3 = sc.textFile(args.model()+"/part-00002")
    .map(l => {
    	val pair = l.substring(1, l.length-1).split(",")
    	(pair(0).toInt, pair(1).toDouble)
	})
    .collectAsMap
    val broadcastModel3 = sc.broadcast(model3)

    val broadcastMethod = sc.broadcast(args.method())


    // preds
    testData.map(l =>{
        val arr = l.split(" ")
        val features = arr.drop(2).map(e => e.toInt)
        var total_score1 = 0d
        var total_score2 = 0d
        var total_score3 = 0d
        features.foreach(f => {
            if (broadcastModel1.value.contains(f))
                total_score1 += broadcastModel1.value(f.toInt)
            if (broadcastModel2.value.contains(f))
                total_score2 += broadcastModel2.value(f.toInt)
            if (broadcastModel3.value.contains(f))
                total_score3 += broadcastModel3.value(f.toInt)
        })

        if (broadcastMethod.value.equals("average")){
            val avg_score = (total_score1 + total_score2 + total_score3) / 3
            val pred = if ( avg_score > 0) "spam" else "ham"
            (arr(0), arr(1), avg_score, pred)
        } else{ // voting
            var votes = 0
            if (total_score1 > 0){
                votes +=1
            }else{
                votes -=1
            }
            if (total_score2 > 0){
                votes +=1
            }else{
                votes -=1
            }
            if (total_score3 > 0){
                votes +=1
            }else{
                votes -=1
            }

            val pred = if ( votes > 0) "spam" else "ham"
            (arr(0), arr(1), votes, pred)
        }
    })
    .saveAsTextFile(args.output())

  }

}