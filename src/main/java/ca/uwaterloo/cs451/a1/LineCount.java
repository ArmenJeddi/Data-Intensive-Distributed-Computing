// Author: Ahmadreza

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
// import tl.lin.data.pair.PairOfStrings;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class LineCount extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(LineCount.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private static final Text WORD = new Text();
    private static final FloatWritable ONE = new FloatWritable(1);

    private int line_limit = 40 ;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      

      WORD.set("*Total-Count");
      context.write(WORD, ONE);

      List<String> orig_tokens = Tokenizer.tokenize(value.toString());
      List<String> tokens = new ArrayList<>();

      for (int i = 0; i < Math.min(orig_tokens.size(), line_limit); i++) {
        if(!tokens.contains(orig_tokens.get(i))){
            tokens.add(orig_tokens.get(i)) ;
            WORD.set(orig_tokens.get(i));
            context.write(WORD, ONE);
        }
      }

    }
  }

  private static final class MyCombiner extends
      Reducer<Text, FloatWritable, Text, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
      
    }
  }


  private static final class MyReducer extends
      Reducer<Text, FloatWritable, Text, FloatWritable> {
    
    private static final FloatWritable VALUE = new FloatWritable();

    private Map<String, Float> unaries = new HashMap<String, Float>() ;

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0  ;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      VALUE.set(sum);
      context.write(key, VALUE);

    }
  }

//   private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
//     @Override
//     public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
//       return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
//     }
//   }

  /**
   * Creates an instance of this tool.
   */
  protected LineCount() {}

  protected static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    boolean textOutput = false;

    @Option(name = "-threshold", required = false, usage = "cooccurence threshold for pair selection")
    public int threshold = 1;
    
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + LineCount.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Job job = Job.getInstance(getConf());
    job.setJobName(LineCount.class.getSimpleName());
    job.setJarByClass(LineCount.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path("line-count");
    FileSystem.get(getConf()).delete(outputDir, true);

    // job.setNumReduceTasks(args.numReducers);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path("line-count"));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    // job.setPartitionerClass(MyPartitioner.class);

    // settings for the datasci cluster
    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LineCount(), args);
  }
}