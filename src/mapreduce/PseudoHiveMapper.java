package mapreduce;

import static mapreduce.PseudoHiveMapper.OmittedRows.MALFORMED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.ORPHANED_RECORDS;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import operators.Operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import database.Row;

public class PseudoHiveMapper extends Mapper<LongWritable, Text, StringArrayWritable, StringArrayWritable> {
  public static final String SELECT_OPERATORS = "select_operators";
  public static final String GROUP_BY_OPERATORS = "group_by_operators";
  public static final String MAIN_TABLE_ROW_CLASS_NAME = "main_table_row_class_name";
  public static final String MAIN_TABLE_JOIN_KEY = "main_table_join_key";
  public static final String USE_JOIN_TABLE = "use_join_table";

  public enum OmittedRows {
    MALFORMED_RECORDS, ORPHANED_RECORDS
  };

  Map<Object, Row> joinedTable;
  int mainTableJoinKey;
  boolean useJoinTable;

  List<Operator> groupByOperators, selectOperators;
  Class<? extends Row> mainTableRowClass;

  @SuppressWarnings("unchecked")
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    useJoinTable = conf.getBoolean(USE_JOIN_TABLE, false);
    mainTableJoinKey = conf.getInt(MAIN_TABLE_JOIN_KEY, 0);
    getMainTableRowClass(conf);

    XStream xstream = new XStream(new StaxDriver());
    groupByOperators = (List<Operator>) xstream.fromXML(conf.get(GROUP_BY_OPERATORS));
    selectOperators = (List<Operator>) xstream.fromXML(conf.get(SELECT_OPERATORS));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    try {
      map(value, context);
    } catch (MalformedRecordException e) {
      context.getCounter(MALFORMED_RECORDS).increment(1);
    } catch (OrphanedRecordException e) {
      context.getCounter(ORPHANED_RECORDS).increment(1);
    }
  }

  protected void map(Text value, Context context) throws MalformedRecordException, OrphanedRecordException, IOException,
      InterruptedException {
    Row row = instantiateNewRow();
    parseValue(value, row);

    Row joinedRow = getJoinedRow(row);

    StringArrayWritable outputKey = mapRowsByOperators(row, joinedRow, groupByOperators);
    StringArrayWritable outputValue = mapRowsByOperators(row, joinedRow, selectOperators);
    context.write(outputKey, outputValue);
  }

  protected StringArrayWritable mapRowsByOperators(Row row, Row joinedRow, List<Operator> operators) {
    StringArrayWritable result = new StringArrayWritable();
    for (Operator operator : operators) {
      result.addAll(operator.map(row, joinedRow));
    }
    return result;
  }

  private Row getJoinedRow(Row row) throws OrphanedRecordException {
    if (useJoinTable) {
      Row joinedRow = joinedTable.get(row.get(mainTableJoinKey));
      if (joinedRow == null) {
        throw new OrphanedRecordException();
      }
      return joinedRow;
    }
    return null;
  }

  protected void parseValue(Text value, Row row) throws MalformedRecordException {
    boolean readSuccess = row.readFromLine(value.toString());
    if (readSuccess) {
      readSuccess = row.isValid();
    }
    if (readSuccess == false) {
      throw new MalformedRecordException();
    }
  }

  private Row instantiateNewRow() {
    try {
      return mainTableRowClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  protected void getMainTableRowClass(Configuration conf) {
    try {
      mainTableRowClass = (Class<? extends Row>) Class.forName(conf.get(MAIN_TABLE_ROW_CLASS_NAME));
    } catch (ClassNotFoundException e) {
      new RuntimeException(e);
    }
  }

  private class MalformedRecordException extends Exception {
    private static final long serialVersionUID = 931758288441250318L;
  }

  private class OrphanedRecordException extends Exception {
    private static final long serialVersionUID = -9142462592805587297L;
  }
}
