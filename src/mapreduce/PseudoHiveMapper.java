package mapreduce;

import static mapreduce.PseudoHiveMapper.OmittedRows.MALFORMED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.ORPHANED_RECORDS;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import operators.Operator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import database.Row;

public class PseudoHiveMapper extends Mapper<LongWritable, Text, StringArrayWritable, StringArrayWritable> {
  public enum OmittedRows {
    MALFORMED_RECORDS, ORPHANED_RECORDS
  };

  Map<Object, Row> joinedTable;
  int mainTableJoinKey;
  boolean useJoinTable;

  List<Operator> groupByOperators, selectOperators;
  Class<? extends Row> mainTableRowClass;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {}

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

  private class MalformedRecordException extends Exception {
  }

  private class OrphanedRecordException extends Exception {
  }
}
