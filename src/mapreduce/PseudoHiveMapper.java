package mapreduce;

import static mapreduce.PseudoHiveMapper.OmittedRows.FILTERED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.MALFORMED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.ORPHANED_RECORDS;
import static tools.RowParser.instantiateNewRow;
import static tools.RowParser.parseValue;
import static tools.Settings.FILTERS;
import static tools.Settings.GROUP_BY_OPERATORS;
import static tools.Settings.JOINED_TABLE_HASH_MAP;
import static tools.Settings.MAIN_TABLE_JOIN_KEY;
import static tools.Settings.MAIN_TABLE_ROW_CLASS_NAME;
import static tools.Settings.SELECT_OPERATORS;
import static tools.Settings.USE_JOIN_TABLE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import operators.Operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tools.RowParser.MalformedRecordException;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

import database.Row;
import filters.Filter;

public class PseudoHiveMapper extends Mapper<LongWritable, Text, StringArrayWritable, StringArrayWritable> {

  public enum OmittedRows {
    MALFORMED_RECORDS, ORPHANED_RECORDS, FILTERED_RECORDS
  };

  Map<Object, Row> joinedTable;
  int mainTableJoinKey;
  boolean useJoinTable;

  List<? extends Operator> groupByOperators, selectOperators;
  List<? extends Filter> filters;
  Class<? extends Row> mainTableRowClass;

  private XStream xstream = new XStream(new StaxDriver());

  @SuppressWarnings("unchecked")
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    useJoinTable = conf.getBoolean(USE_JOIN_TABLE, false);
    mainTableJoinKey = conf.getInt(MAIN_TABLE_JOIN_KEY, 0);
    mainTableRowClass = getRowClass(conf.get(MAIN_TABLE_ROW_CLASS_NAME));

    groupByOperators = (List<? extends Operator>) xstream.fromXML(conf.get(GROUP_BY_OPERATORS));
    selectOperators = (List<? extends Operator>) xstream.fromXML(conf.get(SELECT_OPERATORS));
    filters = (List<? extends Filter>) xstream.fromXML(conf.get(FILTERS));

    loadJoinedTableFromDistributedCache(conf);
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
    } catch (FilteredRecordException e) {
      context.getCounter(FILTERED_RECORDS).increment(1);
    }
  }

  protected void map(Text value, Context context) throws MalformedRecordException, OrphanedRecordException, IOException,
      InterruptedException, FilteredRecordException {
    Row row = instantiateNewRow(mainTableRowClass);
    parseValue(value.toString(), row);

    Row joinedRow = getJoinedRow(row);

    filterRecord(row, joinedRow);
    StringArrayWritable outputKey = mapRowsByOperators(row, joinedRow, groupByOperators);
    StringArrayWritable outputValue = mapRowsByOperators(row, joinedRow, selectOperators);
    context.write(outputKey, outputValue);
  }

  protected void filterRecord(Row row, Row joinedRow) throws FilteredRecordException {
    for (Filter filter : filters) {
      if (filter.filter(row, joinedRow) == false) {
        throw new FilteredRecordException();
      }
    }
  }

  protected StringArrayWritable mapRowsByOperators(Row row, Row joinedRow, List<? extends Operator> operators) {
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

  @SuppressWarnings("unchecked")
  protected Class<? extends Row> getRowClass(String className) {
    try {
      return (Class<? extends Row>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      new RuntimeException(e);
    }
    return null;
  }

  protected void loadJoinedTableFromDistributedCache(Configuration conf) throws IOException {
    if (useJoinTable) {
      loadJoinTableFrom(conf, new Path(JOINED_TABLE_HASH_MAP));
    }
  }

  @SuppressWarnings("unchecked")
  protected void loadJoinTableFrom(Configuration conf, Path path) throws IOException {
    FSDataInputStream joinedTableInHdfs = FileSystem.get(conf).open(path);
    joinedTable = (Map<Object, Row>) xstream.fromXML(joinedTableInHdfs);
    joinedTableInHdfs.close();
  }

  private class OrphanedRecordException extends Exception {
    private static final long serialVersionUID = -9142462592805587297L;
  }

  private class FilteredRecordException extends Exception {
    private static final long serialVersionUID = -5364511061055717887L;
  }
}
