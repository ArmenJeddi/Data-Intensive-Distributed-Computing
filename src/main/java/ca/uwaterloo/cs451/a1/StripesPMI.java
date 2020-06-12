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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable ;
import tl.lin.data.pair.PairOfFloatInt ;

public class StripesPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  protected static class MyMapper extends Mapper<LongWritable, Text, Text, HashMapWritable> {

    private static final Text TEXT = new Text();
    private static final PairOfFloatInt ONE = new PairOfFloatInt(1.0f, 1);


    private int line_limit = 40 ;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      Map<String, HashMapWritable> stripes = new HashMap<>();
      
      List<String> orig_tokens = Tokenizer.tokenize(value.toString());
      List<String> tokens = new ArrayList<>();

      for (int i = 0; i < Math.min(orig_tokens.size(), line_limit); i++) {
        if(!tokens.contains(orig_tokens.get(i))){
            tokens.add(orig_tokens.get(i)) ;
        }
      }

      if (tokens.size() < 2) return;

      for (int i = 0; i < tokens.size(); i++){
        for (int j = 0; j < tokens.size(); j++){

            if (i == j) continue ;
          
            if (stripes.containsKey(tokens.get(i))) {
                HashMapWritable<Text, PairOfFloatInt> stripe = stripes.get(tokens.get(i));
                
                stripe.put(new Text(tokens.get(j)), ONE);
                
            } else {
                HashMapWritable<Text, PairOfFloatInt> stripe = new HashMapWritable<>();
                stripe.put(new Text(tokens.get(j)), ONE);
                stripes.put(tokens.get(i), stripe);
            }
        }
      }

      for (String t : stripes.keySet()) {
        TEXT.set(t);
        context.write(TEXT, stripes.get(t));
      }

    }
      
  }

  protected static class MyCombiner extends
      Reducer<Text, HashMapWritable, Text, HashMapWritable> {

    @Override
    public void reduce(Text key, Iterable<HashMapWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HashMapWritable> iter = values.iterator();
      HashMapWritable<Text, PairOfFloatInt> map = new HashMapWritable<>();

      while (iter.hasNext()) {
        HashMapWritable<Text, PairOfFloatInt> record = iter.next();
        for(Text key_num: record.keySet()){
            PairOfFloatInt hash_val = record.get(key_num) ;

            if (!map.containsKey(key_num)) {
                map.put(key_num, hash_val);
            }else{
                PairOfFloatInt existing = map.get(key_num) ;
                existing.set(existing.getLeftElement() + hash_val.getLeftElement(), existing.getRightElement() + hash_val.getRightElement()) ;
            }
        }

      }

      context.write(key, map);;
    }
  }


  private static final class MyReducer extends
      Reducer<Text, HashMapWritable, Text, HashMapWritable> {
    
    private static final FloatWritable VALUE = new FloatWritable();

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
    public void reduce(Text key, Iterable<HashMapWritable> values, Context context)
        throws IOException, InterruptedException {
        
        Iterator<HashMapWritable> iter = values.iterator();
        HashMapWritable<Text, PairOfFloatInt> map = new HashMapWritable<>();
        HashMapWritable<Text, PairOfFloatInt> map_after_threshold = new HashMapWritable<>();

        while (iter.hasNext()) {
            HashMapWritable<Text, PairOfFloatInt> record = iter.next();
            for(Text key_num: record.keySet()){
                PairOfFloatInt hash_val = record.get(key_num) ;

                if (!map.containsKey(key_num)) {
                    map.put(key_num, hash_val);
                }else{
                    PairOfFloatInt existing = map.get(key_num) ;
                    existing.set(existing.getLeftElement() + hash_val.getLeftElement(), existing.getRightElement() + hash_val.getRightElement()) ;
                }
            }

        }

        Configuration conf = context.getConfiguration();
        int threshold = Integer.parseInt(conf.get("threshold")) ;

        for (Text term : map.keySet()) {
            //map.put(term, map.get(term) / sum);

            if (map.get(term).getRightElement() >= threshold){

                float total = unaries.get("*Total-Count");

                float coprob = map.get(term).getRightElement()/total;

                float leftprob =   unaries.get(key.toString()) /total;


                float rightprob = unaries.get(term.toString() ) / total;

                float pmi = (float)Math.log10(coprob / (leftprob * rightprob));

                map_after_threshold.put(term, new PairOfFloatInt(pmi, map.get(term).getRightElement()));
            }

        }

        if (map_after_threshold.size() > 0)
            context.write(key, map_after_threshold);
    }
  }
  
  protected static class MyPartitioner extends Partitioner<Text, HashMapWritable> {
    @Override
    public int getPartition(Text key, HashMapWritable value, int numReduceTasks) {
      return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

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

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = new Configuration();
    conf.set("threshold", Integer.toString(args.threshold));

    Job job = new Job(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HashMapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HashMapWritable.class);
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
    ToolRunner.run(new StripesPMI(), args);

    System.out.println("Job Finished in " + (System.currentTimeMillis() - overall_time) / 1000.0 + " seconds");

  }

}