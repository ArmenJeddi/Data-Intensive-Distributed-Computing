package ca.uwaterloo.cs451.a4;

import org.apache.hadoop.io.Writable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

//added by me
import java.util.Arrays;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Ahmadreza Jeddi
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

  private Type type;
  private int nodeid;
  private float[] pageranks ;
  private ArrayListOfIntsWritable adjacencyList;
  private int numSources ;
  private int[] sources ;

  public PageRankNode() {}

  public float getSourcePageRankByIndex(int index) {
    return pageranks[index];
  }

  public void setSources(int[] sources) {
    this.sources = sources;
    this.numSources = sources.length ;
    this.pageranks = new float[this.numSources] ;
    // for(int i =0; i < numSources; i++){
    //     setSourcePageRankByIndex(i, (float) StrictMath.log(0));
    // }
  }

  public float[] getPageRankArray(){
    return this.pageranks ;
  }

  public void setSourcePageRankByIndex(int index, float p) {
    pageranks[index] = p ;
  }

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    this.nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacencyList() {
    return adjacencyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    this.adjacencyList = list;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   * @throws IOException if any exception is encountered during object deserialization
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    type = mapping[b];
    nodeid = in.readInt();
    numSources = in.readInt() ;

    sources = new int[numSources] ;
    pageranks = new float[numSources] ;
    for (int i = 0 ; i < numSources; i++){
        int sourceId = in.readInt() ;
        float pagerank = in.readFloat() ;
        sources[i] = sourceId ;
        pageranks[i] = pagerank ;
    }

    if (type.equals(Type.Mass)) 
      return;

    adjacencyList = new ArrayListOfIntsWritable();
    adjacencyList.readFields(in);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   * @throws IOException if any exception is encountered during object serialization
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);
    out.writeInt(numSources);

    for (int i = 0; i < numSources; i++){
        out.writeInt(sources[i]) ;
        out.writeFloat(pageranks[i]) ;
    }

    if (type.equals(Type.Mass))
      return;

    adjacencyList.write(out);
  }

  @Override
  public String toString() {
    return String.format("{%d %s %s}", nodeid, Arrays.toString(pageranks), (adjacencyList == null ? "[]"
        : adjacencyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException if any exception is encountered during object serialization
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}