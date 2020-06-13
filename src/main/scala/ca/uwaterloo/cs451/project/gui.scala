package ca.uwaterloo.cs451.project

import scala.swing._ 
// import ScalaRangeSlider
import scala.swing.event._ 
import javax.swing.{JSlider, JLabel}

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedInputStream, OutputStreamWriter}
import java.text.DecimalFormat

import ca.uwaterloo.cs451.lib.RangeSlider


class ScalaRangeSlider extends Component with Orientable.Wrapper with Publisher {
	override lazy val peer: RangeSlider = new RangeSlider with SuperMixin
	 
	def min: Int = peer.getMinimum
	def min_=(v: Int) { peer.setMinimum(v) }
	def max: Int = peer.getMaximum
	def max_=(v: Int) { peer.setMaximum(v) }
	def value: Int = peer.getValue
    def value_=(v: Int) { peer.setValue(v) }
	def extent: Int = peer.getExtent
	def extent_=(v: Int) { peer.setExtent(v) }
	 
	def paintLabels: Boolean = peer.getPaintLabels
	def paintLabels_=(v: Boolean) { peer.setPaintLabels(v) }
	def paintTicks: Boolean = peer.getPaintTicks
	def paintTicks_=(v: Boolean) { peer.setPaintTicks(v) }
	def paintTrack: Boolean = peer.getPaintTrack
	def paintTrack_=(v: Boolean) { peer.setPaintTrack(v) }
	 
	def snapToTicks: Boolean = peer.getSnapToTicks
	def snapToTicks_=(v: Boolean) { peer.setSnapToTicks(v) }
	 
	def minorTickSpacing: Int = peer.getMinorTickSpacing
	def minorTickSpacing_=(v: Int) { peer.setMinorTickSpacing(v) }
	def majorTickSpacing: Int = peer.getMajorTickSpacing
	def majorTickSpacing_=(v: Int) { peer.setMajorTickSpacing(v) }
	 
	def adjusting = peer.getValueIsAdjusting

	def labels_=(l: scala.collection.Map[Int, Label]) {
	    // TODO: do some lazy wrapping
	    val table = new java.util.Hashtable[Any, Any]
	    for ((k,v) <- l) table.put(k, v.peer)
	    peer.setLabelTable(table)
	}
	 
	peer.addChangeListener(new javax.swing.event.ChangeListener {
	    def stateChanged(e: javax.swing.event.ChangeEvent) { 
	      publish(new ValueChanged(ScalaRangeSlider.this)) 
	    }
	})
}




class UI() extends MainFrame {
  private def restrictHeight(s: Component) {
    s.maximumSize = new Dimension(Short.MaxValue, s.preferredSize.height)
  }

  title = "Graph query platform"

  val edgeField = new TextField { columns = 40 }
  val graphOutField = new TextField { columns = 40 }
  val buildButton = new Button("build")

  restrictHeight(edgeField)
  restrictHeight(graphOutField)

  val edgePannel = new BoxPanel(Orientation.Horizontal){
      contents += new Label("Input edge file")
      contents += Swing.HStrut(20)
      contents += edgeField
  }

  val graphOutPannel = new BoxPanel(Orientation.Horizontal){
      contents += new Label("Path to output")
      contents += Swing.HStrut(20)
      contents += graphOutField
  }

  val weightedGraph = new CheckBox("Weighted graph")
  weightedGraph.selected = false

  val undirectedGraph = new CheckBox("Undirected graph")
  undirectedGraph.selected = false

  val buildGraphField = new BoxPanel(Orientation.Vertical){
      contents += new BoxPanel(Orientation.Horizontal) {
          contents += Swing.HStrut(50)
          contents += new Label("Build a new User Graph!")
      }
      contents += Swing.VStrut(10)
      contents += edgePannel
      contents += Swing.VStrut(10)
      contents += graphOutPannel
      contents += Swing.VStrut(10)
      contents += weightedGraph
      contents += Swing.VStrut(10)
      contents += undirectedGraph
      contents += Swing.VStrut(15)
      contents += buildButton
  }

  // Search Section

  val graphPathField = new TextField { columns = 40 }
  restrictHeight(graphPathField)

  val graphPathPannel = new BoxPanel(Orientation.Horizontal){
      contents += new Label("Path to load the graph")
      contents += Swing.HStrut(20)
      contents += graphPathField
  }

  val ageSlider = new ScalaRangeSlider {
      min = 18
      max = 75
      value = 30
      extent = 20
      val hMap = scala.collection.Map[Int, Label](18 -> new Label("18"), 18 -> new Label("18") , 30 -> new Label("30"), 45 -> new Label("45"), 60 -> new Label("60"), 75 -> new Label("75"))
      labels_=(hMap)
      this.peer.setPaintLabels(true)
  }
  restrictHeight(ageSlider)

  val ageSliderPannel = new BoxPanel(Orientation.Horizontal){
      contents += new Label("Age bound")
      contents += Swing.HStrut(20)
      contents += ageSlider
      
  }

  val searchNode = new TextField { 
      columns = 40
    }
  restrictHeight(searchNode)

  val searchNodePannel = new BoxPanel(Orientation.Horizontal){
      contents += new Label("Node to search from")
      contents += Swing.HStrut(20)
      contents += searchNode
  }

  val usePageRank = new CheckBox("Use Pagerank")
  usePageRank.selected = true

  val searchButton = new Button("Search")
  val resultField = new TextArea { 
    rows = 15
    lineWrap = true
    wordWrap = true
    editable = false
  }

  val searchSection = new BoxPanel(Orientation.Vertical){
      contents += new BoxPanel(Orientation.Horizontal) {
          contents += Swing.HStrut(50)
          contents += new Label("Search for network users from a user's point of view")
      }
      contents += Swing.VStrut(10)
      contents += graphPathPannel
      contents += Swing.VStrut(10)
      contents += searchNodePannel
      contents += Swing.VStrut(10)
      contents += ageSliderPannel
      contents += Swing.VStrut(10)
      contents += usePageRank
      contents += Swing.VStrut(15)
      contents += searchButton
      contents += Swing.VStrut(15)
      contents += new ScrollPane(resultField)
      usePageRank.xLayoutAlignment = 0.0
    //   for (e <- contents) {
    //       if (e.isInstanceof[CheckBox])
    //         e.xLayoutAlignment = 0.0
    //   }
  }
  
  contents = new BoxPanel(Orientation.Vertical) {
    contents += buildGraphField
    contents += Swing.VStrut(120)
    contents += searchSection
    border = Swing.EmptyBorder(10, 10, 10, 10)
  }

  listenTo(buildButton)
  listenTo(searchButton)
  reactions += {
    case ButtonClicked(`buildButton`) => {
        val inputEdgePath = edgeField.text 
        val outputPath = graphOutField.text
        val weightedSelected = weightedGraph.selected
        val undirectedSelected = undirectedGraph.selected
        if (inputEdgePath.isEmpty || outputPath.isEmpty){
            Dialog.showMessage(contents.head, "Input required fields", title="Warning!")
        } else{
            val processBuilder = new ProcessBuilder()
            val spark_cmd = "spark-submit --driver-memory 12g --class ca.uwaterloo.cs451.project.BuildGraph target/assignments-1.0.jar"
            val input_arg = "--input " + inputEdgePath
            val output_arg = "--output " + outputPath
            val weightedCmd = if(weightedSelected) "--weighted" else ""
            val undirectedCmd = if(undirectedSelected) "--undirected" else ""
            processBuilder.command("bash", "-c", s"$spark_cmd $input_arg $output_arg $weightedCmd $undirectedCmd")
            try{
                val process = processBuilder.start()
                val reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
                val exitCode = process.waitFor()
                if (exitCode == 0){
                    Dialog.showMessage(contents.head, "Graph is successfully generated and saved!", title="Congrats!")
                }else{
                    Dialog.showMessage(contents.head, "Input file does not exist!", title="Error!")
                }
            }catch{
                case e: Exception => Dialog.showMessage(contents.head, "There are exceptions happening!", title="Error!")
            }
        }
    }
    case ButtonClicked(`searchButton`) => {
        val graph_dir = graphPathField.text
        val srcNode = searchNode.text
        val startAGe = ageSlider.value
        val endAge = startAGe + ageSlider.extent
        val pagerankSelected = usePageRank.selected
        if (graph_dir.isEmpty || srcNode.isEmpty){
            Dialog.showMessage(contents.head, "Input required fields", title="Warning!")
        } else{
            val processBuilder = new ProcessBuilder()
            val spark_cmd = "spark-submit --driver-memory 12g --class ca.uwaterloo.cs451.project.GraphQuery target/assignments-1.0.jar"
            val input_arg = "--input " + graph_dir
            val srcnode_arg = "--srcnode " + srcNode
            val agerange_arg =  "--agerange " + s"$startAGe-$endAge"
            if (pagerankSelected){
                processBuilder.command("bash", "-c", s"$spark_cmd $input_arg $srcnode_arg $agerange_arg --pagerank")
                println(s"$spark_cmd $input_arg $srcnode_arg $agerange_arg --pagerank")
            }
            else{
                processBuilder.command("bash", "-c", s"$spark_cmd $input_arg $srcnode_arg $agerange_arg")
                println(s"$spark_cmd $input_arg $srcnode_arg $agerange_arg")
            }
            try{
                val srctoLong = srcNode.toLong
                val process = processBuilder.start()
                val reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
                val exitCode = process.waitFor()
                if (exitCode == 0){
                    Dialog.showMessage(contents.head, "Qyery was successful!", title="Congrats!")
                    val hdfs = FileSystem.get(new Configuration())
                    val is = new BufferedInputStream( hdfs.open( new Path( "./rank-temp/part-00000" ) ) )
                    val lines = scala.io.Source.fromInputStream( is ).getLines()
                    val df2 = new DecimalFormat("#.######")
                    // val lines = scala.io.Source.fromFile("./rank-temp/part-00000").getLines
                    val queryText = lines.map(p=>{
                        val arr = p.replace("(", "").replace(")", "").split(",")
                        val id = arr(0)
                        val rank = arr(1).toDouble
                        val age = arr(2)
                        val fields = arr(3).replaceAll("-", ", ")
                        val output = id + "\t  " + df2.format(rank) + "\t  " + age + "\t  " + fields
                        output
                    })
                    val queryResult = queryText mkString "\n"
                    val dashLine = "--------------\t  -------------------\t  ------\t  --------------\n"
                    val finalText = "vertex ID\t  search rank\t  age\t  interests\n" + dashLine + queryResult
                    resultField.text = finalText
                    resultField.caret.position = 0

                }else{
                    Dialog.showMessage(contents.head, "Input file does not exist!", title="Error!")
                }
            }catch{
                case e: NumberFormatException => Dialog.showMessage(contents.head, "source node is not valid", title="Error!")
                case _ => Dialog.showMessage(contents.head, "There are exceptions happening!", title="Error!")
            }
        }

    }
  }
  
}

object GraphxSystem {
  def main(args: Array[String]) {
    val ui = new UI()
    ui.visible = true
  }
}
