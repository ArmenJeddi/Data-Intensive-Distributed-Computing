// Author: Ahmadreza

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;

// new data types
import tl.lin.data.pair.PairOfFloatInt ;


public class PairsPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  protected static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt ONE = new PairOfFloatInt(1.0f, 1);
    private static final PairOfStrings Pair = new PairOfStrings();

    private int line_limit = 40 ;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      List<String> orig_tokens = Tokenizer.tokenize(value.toString());
      List<String> tokens = new ArrayList<>();

      for (int i = 0; i < Math.min(orig_tokens.size(), line_limit); i++) {
        if(!tokens.contains(orig_tokens.get(i))){
            tokens.add(orig_tokens.get(i)) ;
        }
      }

      if (tokens.size() < 2) return;

      for (int i = 0; i < tokens.size() - 1; i++){
        for (int j = i + 1; j < tokens.size(); j++){
          Pair.set(tokens.get(i), tokens.get(j));
          context.write(Pair, ONE);
          Pair.set(tokens.get(j), tokens.get(i));
          context.write(Pair, ONE);
        }
      }

    }
      
  }

  protected static class MyCombiner extends
      Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt PMI_SUM = new PairOfFloatInt();

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<PairOfFloatInt> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().getValue();
      }
      PMI_SUM.set((float) sum, sum);
      context.write(key, PMI_SUM);
    }
  }


  private static final class MyReducer extends
      Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
    
    private static final PairOfFloatInt VALUE = new PairOfFloatInt();

    private Map<String, Float> unaries = new HashMap<String, Float>() ;


    @Override
    public void setup (Context context) throws IOException{
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            Path inFile = new Path("./line-count/part-r-00000");
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));

            String line;
            line = br.readLine();
            while (line != null){
                
                String[] line_parts = new String[2];
                
                line_parts = line.split("\\s+");

                String counted_str = line_parts[0] ;
                float count = Float.parseFloat(line_parts[1]) ;

                unaries.put(counted_str, count) ;

                line = br.readLine();
            }

            br.close() ;


        } catch(IOException e){
            System.out.println("The path to the line count was not found");
        }

    }


    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0 ;
        Iterator<PairOfFloatInt> iter = values.iterator();
        while (iter.hasNext()) {
            sum += iter.next().getValue();
        }

        Configuration conf = context.getConfiguration();
        int threshold = Integer.parseInt(conf.get("threshold")) ;
        
        // System.out.printf("(%s %s) , %f\n", key.getLeftElement(), key.getRightElement(), sum) ;
        if (sum >= threshold){
          float total_lines = unaries.get("*Total-Count") ;
          float left = unaries.get(key.getLeftElement()) ;
          float right = unaries.get(key.getRightElement()) ;

          float left_prob = left / total_lines ;
          float right_prob = right / total_lines ;

          float pair_prob = sum / total_lines ;

          float pmi =  (float)(Math.log10( pair_prob / (left_prob * right_prob))) ;
          
          VALUE.set(pmi, sum);
          context.write(key, VALUE);
        }
        
        // VALUE.set(sum);
        // context.write(key, VALUE);
    }
  }
  
  protected static class MyPartitioner extends Partitioner<PairOfStrings, PairOfFloatInt> {
    @Override
    public int getPartition(PairOfStrings key, PairOfFloatInt value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-textOutput", required = false, usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    public boolean textOutput = false;

    @Option(name = "-threshold", required = false, usage = "cooccurence threshold for pair selection")
    public int threshold = 1;

  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = new Configuration();
    conf.set("threshold", Integer.toString(args.threshold));

    Job job = new Job(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);
    
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(PairOfFloatInt.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(PairOfFloatInt.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

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
   */
  public static void main(String[] args) throws Exception {
    long overall_time = System.currentTimeMillis();
    ToolRunner.run(new LineCount(), args);
    ToolRunner.run(new PairsPMI(), args);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - overall_time) / 1000.0 + " seconds");
  }
}