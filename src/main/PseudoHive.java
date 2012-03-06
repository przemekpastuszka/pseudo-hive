package main;

import static tools.RowParser.instantiateNewRow;
import static tools.RowParser.parseValue;
import static tools.Settings.FILTERS;
import static tools.Settings.GROUP_BY_OPERATORS;
import static tools.Settings.JOINED_TABLE_HASH_MAP;
import static tools.Settings.MAIN_TABLE_JOIN_KEY;
import static tools.Settings.MAIN_TABLE_ROW_CLASS_NAME;
import static tools.Settings.SELECT_OPERATORS;
import static tools.Settings.USE_JOIN_TABLE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import mapreduce.PseudoHiveCombiner;
import mapreduce.PseudoHiveMapper;
import mapreduce.PseudoHiveReducer;
import mapreduce.StringArrayWritable;
import operators.ExtendedOperator;
import operators.Operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.RowParser.MalformedRecordException;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import database.Row;
import filters.Filter;

public class PseudoHive {

  Path mainInput, joinInput, output;
  List<? extends Operator> groupByOperators = new LinkedList<Operator>();
  List<? extends ExtendedOperator> selectOperators = new LinkedList<ExtendedOperator>();
  List<? extends Filter> filters = new LinkedList<Filter>();
  String mainTableRowClassName;

  Class<? extends Row> joinedTableRowClass;
  int joinTableKeyId, mainTableKeyId;
  boolean useJoinTable = false;

  public void setMainInput(Path mainInput) {
    this.mainInput = mainInput;
  }

  public void setJoinInput(Path joinInput) {
    this.joinInput = joinInput;
  }

  public void setOutput(Path output) {
    this.output = output;
  }

  public void setGroupByOperators(List<? extends Operator> groupByOperators) {
    this.groupByOperators = groupByOperators;
  }

  public void setSelectOperators(List<? extends ExtendedOperator> selectOperators) {
    this.selectOperators = selectOperators;
  }

  public void setMainRowClass(Class<? extends Row> clazz) {
    mainTableRowClassName = clazz.getName();
  }

  public void setJoinedRowClass(Class<? extends Row> clazz) {
    joinedTableRowClass = clazz;
  }

  public void setJoinKeys(Enum<?> main, Enum<?> joined) {
    mainTableKeyId = main.ordinal();
    joinTableKeyId = joined.ordinal();
    useJoinTable = true;
  }

  public void setFilters(List<? extends Filter> filters) {
    this.filters = filters;
  }

  private XStream xstream = new XStream(new StaxDriver());

  public int run(Configuration conf) throws Exception {
    setUpJoin(conf);

    conf.set(MAIN_TABLE_ROW_CLASS_NAME, mainTableRowClassName);
    conf.set(GROUP_BY_OPERATORS, xstream.toXML(groupByOperators));
    conf.set(SELECT_OPERATORS, xstream.toXML(selectOperators));
    conf.set(FILTERS, xstream.toXML(filters));

    Job job = new Job(conf);
    job.setJarByClass(PseudoHive.class);

    FileInputFormat.addInputPath(job, mainInput);
    FileOutputFormat.setOutputPath(job, output);

    job.setMapperClass(PseudoHiveMapper.class);
    job.setCombinerClass(PseudoHiveCombiner.class);
    job.setReducerClass(PseudoHiveReducer.class);

    job.setMapOutputKeyClass(StringArrayWritable.class);
    job.setMapOutputValueClass(StringArrayWritable.class);

    job.setOutputKeyClass(StringArrayWritable.class);
    job.setOutputValueClass(StringArrayWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  protected void setUpJoin(Configuration conf) throws IOException, URISyntaxException {
    conf.setBoolean(USE_JOIN_TABLE, useJoinTable);
    conf.setInt(MAIN_TABLE_JOIN_KEY, mainTableKeyId);

    if (useJoinTable) {
      System.out.println("Preparing join table...");
      FileSystem fs = FileSystem.get(conf);
      Map<Object, Row> joinedTable = prepareJoinHashMapFromFile(fs);
      storeJoinHashMapInHdfs(fs, joinedTable);
      DistributedCache.addCacheFile(new URI(JOINED_TABLE_HASH_MAP), conf);
      fs.deleteOnExit(new Path(JOINED_TABLE_HASH_MAP));
    }
  }

  protected void storeJoinHashMapInHdfs(FileSystem fs, Map<Object, Row> joinedTable) throws IOException {
    FSDataOutputStream joinedTableInHdfs = fs.create(new Path(JOINED_TABLE_HASH_MAP));
    xstream.toXML(joinedTable, joinedTableInHdfs);
    joinedTableInHdfs.close();
  }

  protected Map<Object, Row> prepareJoinHashMapFromFile(FileSystem fs) throws IOException {
    Map<Object, Row> joinedTable = new HashMap<Object, Row>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(joinInput)));
    addLinesFromReaderToJoinHashTable(reader, joinedTable);
    reader.close();

    return joinedTable;
  }

  protected void addLinesFromReaderToJoinHashTable(BufferedReader reader, Map<Object, Row> joinedTable) throws IOException {
    int malformedRecords = 0;
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      try {
        addLineToJoinHashMap(line, joinedTable);
      } catch (MalformedRecordException e) {
        ++malformedRecords;
      }
    }
    System.out.println("Found " + malformedRecords + " malformed records in join table.");
  }

  protected void addLineToJoinHashMap(String line, Map<Object, Row> joinedTable) throws MalformedRecordException {
    Row row = instantiateNewRow(joinedTableRowClass);
    parseValue(line, row);
    joinedTable.put(row.get(joinTableKeyId), row);
  }
}
