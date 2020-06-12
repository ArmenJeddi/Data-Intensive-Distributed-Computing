package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// added packages by me
import org.apache.hadoop.io.VIntWritable ;
import org.apache.hadoop.io.VLongWritable ;
import org.apache.hadoop.io.BytesWritable ;
import java.io.ByteArrayOutputStream ;
import java.io.DataOutputStream ;
import org.apache.hadoop.mapreduce.Partitioner;
import tl.lin.data.pair.PairOfStringInt ;


public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {

    private static final Object2IntFrequencyDistribution<String> COUNTS = new Object2IntFrequencyDistributionEntry<>();
    private static final PairOfStringInt Term_DocId = new PairOfStringInt() ;
    private static final VIntWritable VINT = new VIntWritable() ;

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        Term_DocId.set(e.getLeftElement(), (int)docno.get()) ;
        VINT.set(e.getRightElement()) ;
        context.write(Term_DocId, VINT);
        
        // if (e.getLeftElement().equals("outrageous")){
        //     System.out.println() ;
        //     System.out.println((int)docno.get()) ;
        // }
      }

    //   System.out.println("\n\nIn Mapper\n\n");
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {

    private static ArrayListWritable<PairOfWritables<VIntWritable, VIntWritable>> Postings;
    private static BytesWritable BYTES;
    private static Text PreviousTEXT;
    private static VIntWritable PreviousDocNum;
    private static ByteArrayOutputStream byteArrayStream;
    private static DataOutputStream dataStream;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Postings = new ArrayListWritable<>() ;
        BYTES = new BytesWritable() ;
        PreviousTEXT = new Text("") ;
        PreviousDocNum = new VIntWritable(0) ;
        byteArrayStream = new ByteArrayOutputStream();
        dataStream = new DataOutputStream(byteArrayStream);
    }


    @Override
    public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {
    //   System.out.println("\n\nIn reducer\n\n");
      if (!key.getLeftElement().equals(PreviousTEXT.toString()) && PreviousTEXT.getLength() != 0 ){
          for (PairOfWritables<VIntWritable, VIntWritable> pair : Postings){
                // if (PreviousTEXT.toString().equals("outrageous")){
                //     System.out.println() ;
                //     System.out.println(pair.getLeftElement().get() + "  -  " + pair.getRightElement().get()) ;
                // }
              pair.getLeftElement().write(dataStream);
              pair.getRightElement().write(dataStream);
          }
          Postings.clear() ;

          BYTES.set(byteArrayStream.toByteArray(), 0, byteArrayStream.size()) ;
          context.write(PreviousTEXT, BYTES) ;

          PreviousDocNum.set(0) ;
          byteArrayStream.reset() ;
          dataStream.flush() ;

      }
    
      Iterator<VIntWritable> iter = values.iterator();
      

      int tf = 0;
      while (iter.hasNext()) {
        tf += iter.next().get() ;
      }

      VIntWritable TF = new VIntWritable(tf) ;
      VIntWritable GAP  = new VIntWritable(key.getRightElement() - PreviousDocNum.get()) ;

      Postings.add(new PairOfWritables(GAP , TF));
      PreviousTEXT.set(key.getLeftElement());
      PreviousDocNum.set(key.getRightElement());

    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
        for (PairOfWritables<VIntWritable, VIntWritable> pair : Postings){
            pair.getLeftElement().write(dataStream);
            pair.getRightElement().write(dataStream);
        }
        Postings.clear() ;

        BYTES.set(byteArrayStream.toByteArray(), 0, byteArrayStream.size()) ;

        if (PreviousDocNum.get() > 0 ){
            context.write(PreviousTEXT, BYTES) ;
        }

        PreviousDocNum.set(0) ;
        byteArrayStream.reset() ;
        dataStream.flush() ;
    }


  }

  protected static class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
      return (key.getLeftElement().toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

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
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}