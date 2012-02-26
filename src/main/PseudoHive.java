package main;

import static tools.Settings.GROUP_BY_OPERATORS;
import static tools.Settings.MAIN_TABLE_ROW_CLASS_NAME;
import static tools.Settings.SELECT_OPERATORS;
import static tools.Settings.USE_JOIN_TABLE;

import java.util.List;

import mapreduce.PseudoHiveMapper;
import mapreduce.PseudoHiveReducer;
import mapreduce.StringArrayWritable;
import operators.ExtendedOperator;
import operators.Operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import database.Row;

public class PseudoHive extends Configured implements Tool {

  Path mainInput, joinInput, output;
  List<? extends Operator> groupByOperators;
  List<? extends ExtendedOperator> selectOperators;
  String mainTableRowClassName;

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

  private XStream xstream = new XStream(new StaxDriver());

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(USE_JOIN_TABLE, false);
    conf.set(MAIN_TABLE_ROW_CLASS_NAME, mainTableRowClassName);
    conf.set(GROUP_BY_OPERATORS, xstream.toXML(groupByOperators));
    conf.set(SELECT_OPERATORS, xstream.toXML(selectOperators));

    Job job = new Job(conf);
    job.setJarByClass(PseudoHive.class);

    FileInputFormat.addInputPath(job, mainInput);
    FileOutputFormat.setOutputPath(job, output);

    job.setMapperClass(PseudoHiveMapper.class);
    // job.setCombinerClass(BigramsCountReducer.class);
    job.setReducerClass(PseudoHiveReducer.class);

    job.setMapOutputKeyClass(StringArrayWritable.class);
    job.setMapOutputValueClass(StringArrayWritable.class);

    job.setOutputKeyClass(StringArrayWritable.class);
    job.setOutputValueClass(StringArrayWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
