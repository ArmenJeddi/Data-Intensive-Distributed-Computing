
package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

// My imports
import java.util.ArrayList;
import org.apache.hadoop.io.BytesWritable ;
import org.apache.hadoop.io.WritableUtils ;
import org.apache.hadoop.fs.FileStatus ;
import java.io.ByteArrayInputStream ;
import java.io.DataInputStream ;
import java.util.Collections;   



public class BooleanRetrievalCompressed extends Configured implements Tool {
  private ArrayList<MapFile.Reader> indexes = new ArrayList<>();
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int num_reducers ;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {

    ArrayList<String> folderNames = new ArrayList<>() ;

    FileStatus[] fileStatus = fs.listStatus(new Path(indexPath));
    for(FileStatus status : fileStatus){
        String abs_path = status.getPath().toString() ;
        String dir_name = abs_path.substring(abs_path.lastIndexOf('/') + 1) ;
        if (dir_name.contains("part-r")){
            folderNames.add(dir_name) ;
        }
    }

    Collections.sort(folderNames) ;
    for (String folderName : folderNames){
        // System.out.println(folderName) ;
        MapFile.Reader index = new MapFile.Reader(new Path(indexPath + "/" + folderName), fs.getConf());
        indexes.add(index) ;
    }



    num_reducers = indexes.size() ;

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text();

    BytesWritable value = new BytesWritable() ;

    int partition_number = (term.toString().hashCode() & Integer.MAX_VALUE) % num_reducers;

    MapFile.Reader curr_index = indexes.get(partition_number) ;

    key.set(term);
    curr_index.get(key, value);

    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

    byte[] bytes = value.getBytes();
    DataInputStream dataStream = new DataInputStream(new ByteArrayInputStream(bytes)) ;

    int previous_doc_num = 0 ;
    int gap ;
    int tf ;

    while(true) {
        try {
            gap = WritableUtils.readVInt(dataStream) ;
            tf = WritableUtils.readVInt(dataStream) ;
        }catch (IOException e){
            break ;
        }

        if (gap < 1 || tf < 1){
            break ;
        }
        
        postings.add(new PairOfInts(previous_doc_num + gap, tf)) ;
        previous_doc_num += gap ;

    }

    return postings;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}