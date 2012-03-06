package mapreduce;

import static database.Airport.Attribute.CITY;
import static database.Flight.Attribute.DELAY;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.ORIGIN;
import static java.util.Arrays.asList;
import static mapreduce.PseudoHiveMapper.OmittedRows.FILTERED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.MALFORMED_RECORDS;
import static mapreduce.PseudoHiveMapper.OmittedRows.ORPHANED_RECORDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import operators.Operator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import database.Airport;
import database.Flight;
import database.Row;
import filters.Filter;

@RunWith(MockitoJUnitRunner.class)
public class PseudoHiveMapperTest {

  @Mock
  Operator constA, distanceSelector, citySelector;
  @Mock
  Context context;
  @Mock
  Counter malformedRecords, orphanedRecords, filteredRecords;
  @Mock
  Filter filter;

  PseudoHiveMapper mapper = new PseudoHiveMapper();

  @Before
  public void setUp() {
    when(constA.map(any(Row.class), any(Row.class))).thenReturn(asList("A"));
    when(distanceSelector.map(any(Row.class), any(Row.class))).thenAnswer(new ColumnSelectorAnswer(0, DISTANCE.ordinal()));
    when(citySelector.map(any(Row.class), any(Row.class))).thenAnswer(new ColumnSelectorAnswer(1, CITY.ordinal()));

    when(filter.filter(any(Row.class), any(Row.class))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Row row = (Row) invocation.getArguments()[0];
        return row.getInt(DELAY) != -20;
      }
    });

    when(context.getCounter(MALFORMED_RECORDS)).thenReturn(malformedRecords);
    when(context.getCounter(ORPHANED_RECORDS)).thenReturn(orphanedRecords);
    when(context.getCounter(FILTERED_RECORDS)).thenReturn(filteredRecords);

    mapper.filters = asList(filter);
    mapper.selectOperators = asList(constA, distanceSelector);
    mapper.mainTableRowClass = Flight.class;
  }

  @Test
  public void shouldMapRowsWithoutJoin() throws IOException, InterruptedException {
    mapper.useJoinTable = false;
    mapper.groupByOperators = asList(constA);

    invokeMapOnSampleValues();

    verifyWriteOnContext(asList("A"), asList("A", "1846"));
    verifyWriteOnContext(asList("A"), asList("A", "1617"));
    verifyWriteOnContext(asList("A"), asList("A", "200"));
    verify(malformedRecords, times(2)).increment(1);
    verify(orphanedRecords, never()).increment(anyInt());
    verify(filteredRecords, times(1)).increment(1);
  }

  @Test
  public void shouldMapRowsCorrectlyWhenJoinIsUsed() throws IOException, InterruptedException {
    prepareMapperForJoinedTables();

    invokeMapOnSampleValues();

    verifyWriteOnContext(asList("Bethel"), asList("A", "1846"));
    verifyWriteOnContext(asList("Dublin"), asList("A", "1617"));
    verify(malformedRecords, times(2)).increment(1);
    verify(orphanedRecords, times(1)).increment(1);
    verify(filteredRecords, times(1)).increment(1);
  }

  protected void invokeMapOnSampleValues() throws IOException, InterruptedException {
    map("1987,10,1,4,1,556,AA,190,247,0B1,ORD,1846,0,2"); // good
    map("1987,10,1,4,1,556,AA,190,247,0B1,ORD,10,0,lala"); // malformed - "lala" as delay
    map("1987,10,1,4,1,577,AA,190,247,0B1,ORD,1846,0,2"); // malformed - arrival time is 5:77
    map("2008,10,31,5,2359,356,B6,739,226,DBN,PSE,1617,0,-13"); // good
    map("2008,10,31,5,2359,356,B6,739,226,DBN,PSE,1400,0,-20"); // filtered out
    map("2008,10,31,5,2359,356,B6,739,226,ZXC,PSE,200,0,-13"); // orphaned - "ZXC" does not match any row in joined table
  }

  private void map(String text) throws IOException, InterruptedException {
    mapper.map(new LongWritable(), new Text(text), context);
  }

  private void verifyWriteOnContext(List<String> a, List<String> b) throws IOException, InterruptedException {
    verify(context).write(new StringArrayWritable(a), new StringArrayWritable(b));
  }

  private void prepareMapperForJoinedTables() {
    mapper.joinedTable = new HashMap<Object, Row>();
    mapper.joinedTable.put("0B1", new Airport("\"0B1\",\"Col. Dyke \",\"Bethel\",\"ME\",\"USA\",44.42506444,-70.80784778"));
    mapper.joinedTable.put("DBN", new Airport("\"DBN\",\"W. H. \\\"Bud\\\" Barron \",\"Dublin\",\"GA\",\"USA\",32.56445806,-82.98525556"));

    mapper.useJoinTable = true;
    mapper.mainTableJoinKey = ORIGIN.ordinal();
    mapper.groupByOperators = asList(citySelector);
  }

  private static class ColumnSelectorAnswer implements Answer<List<String>> {
    int rowId, attributeId;

    public ColumnSelectorAnswer(int rowId, int attributeId) {
      this.rowId = rowId;
      this.attributeId = attributeId;
    }

    @Override
    public List<String> answer(InvocationOnMock invocation) throws Throwable {
      Row row = (Row) invocation.getArguments()[rowId];
      return asList(row.get(attributeId).toString());
    }
  }
}
