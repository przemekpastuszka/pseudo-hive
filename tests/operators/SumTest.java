package operators;

import static database.Flight.Attribute.DELAY;
import static java.util.Arrays.asList;
import static operators.AbstractOperator.RowType.JOINED;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import database.Row;

@RunWith(MockitoJUnitRunner.class)
public class SumTest {

  Sum sum = new Sum(DELAY);

  @Mock
  Row row;

  @Before
  public void setUp() {
    when(row.get(DELAY.ordinal())).thenReturn(13);
  }

  @Test
  public void shouldSelectValidColumnInMap() {
    assertThat(sum.map(row, null)).isEqualTo(asList("13"));
  }

  @Test
  public void shouldSelectValidColumnForJoinedRow() {
    sum = new Sum(DELAY, JOINED);

    assertThat(sum.map(null, row)).isEqualTo(asList("13"));
  }

  @Test
  public void shouldReduceCorrectly() {
    sum.start();
    sum.iterate(asList("13"));
    sum.iterate(asList("-5"));
    sum.iterate(asList("0.01"));

    assertThat(sum.numberOfIterateArguments()).isEqualTo(1);
    assertThat(sum.reduce()).isEqualTo(asList("8.01"));
  }

}
