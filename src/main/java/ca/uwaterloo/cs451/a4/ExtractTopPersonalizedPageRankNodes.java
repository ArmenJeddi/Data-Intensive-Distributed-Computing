package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.ArrayList ;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.io.Text;
import java.util.Iterator;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
 * Hadoop structures for representing the graph.
 * </p>
 *
 * @author Ahmadreza Jeddi
 */
public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends Mapper<IntWritable, PageRankNode, MyPairOfIntFloat, IntWritable> {
    private static final MyPairOfIntFloat PAIR = new MyPairOfIntFloat();
    private static final IntWritable NODEID = new IntWritable();
    
    private String[] splittedSources ;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, MyPairOfIntFloat, IntWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      splittedSources = conf.get("SOURCES_FIELD").split(",");
    }

    @Override
    public void map(IntWritable nId, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      
      for (int i = 0; i < splittedSources.length ; i ++){
          PAIR.set(i, node.getSourcePageRankByIndex(i)) ;
          // System.out.println(node.getSourcePageRankByIndex(i)) ;
          NODEID.set(nId.get()) ;
          context.write(PAIR, NODEID);
      }
    }
  }

  
  private static class MyReducer extends
      Reducer<MyPairOfIntFloat, IntWritable, Text, IntWritable> {

    // private static class FloatIntPair{
    //     private float left ;
    //     private int right ;
    //     public FloatIntPair(float left, int right){
    //         this.left = left ;
    //         this.right = right ;
    //     }
    //     public float getLeftElement(){
    //         return this.left ;
    //     }
    //     public int getRightElement(){
    //         return this.right ;
    //     }
    // }

    private static String[] splittedSources ;
    private static int[] sourceNodes ;
    private static int top_count ;

    // private static ArrayList<FloatIntPair> rankIdPairs = new ArrayList<>() ;
    private static final IntWritable PreviousSourceIndex = new IntWritable() ;
    private static int COUNTER;
    private static final Text PAGARANK = new Text() ;
    private static final IntWritable NODEID = new IntWritable() ;

    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      splittedSources = conf.get("SOURCES_FIELD").split(",");
      sourceNodes = new int[splittedSources.length] ;
      for(int i = 0; i < splittedSources.length ; i ++){
        sourceNodes[i] = Integer.parseInt(splittedSources[i]);
      }

      top_count = conf.getInt("TOP", 10) ;

      PreviousSourceIndex.set(-1) ;
      COUNTER = 0 ;
      
    }

    @Override
    public void reduce(MyPairOfIntFloat pair, Iterable<IntWritable> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> values = iterable.iterator();


      if (pair.getLeftElement() != PreviousSourceIndex.get()){
          if (PreviousSourceIndex.get() > -1){
              System.out.println() ;
          }
          PreviousSourceIndex.set(pair.getLeftElement()) ;
          COUNTER = 0 ;
          PAGARANK.set("Source:") ;
          NODEID.set(sourceNodes[pair.getLeftElement()]) ;
          if (COUNTER < top_count){
            context.write(PAGARANK, NODEID)  ;
            System.out.println("Source: " + NODEID.get()) ;
          }
      }


      while (values.hasNext()) {
        IntWritable val = values.next();
        if (COUNTER < top_count){
            float pagerank = (float) StrictMath.exp(pair.getRightElement()) ;
            String str = String.format("%.5f", pagerank) ;
            PAGARANK.set(str) ;
            NODEID.set(val.get()) ;
            COUNTER ++ ;
            String printed = String.format("%.5f %d", pagerank, val.get()) ;
            System.out.println(printed) ;
            context.write(PAGARANK, NODEID) ;
        }
        else{
            break ;
        }
      }

    }

    // @Override
    // public void cleanup(Context context) throws IOException {

    // }
  }


  public ExtractTopPersonalizedPageRankNodes() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes to be returned").create(TOP));
    options.addOption(OptionBuilder.withArgName("list").hasArg()
        .withDescription("list of sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int top = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceList = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - num tops: " + top);
    LOG.info(" - source list: " + sourceList);

    Configuration conf = getConf();
    // conf.setInt(NODE_CNT_FIELD, n);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("TOP", top);
    conf.set("SOURCES_FIELD", sourceList);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getSimpleName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(MyPairOfIntFloat.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
  }
}