/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package operators;

import static database.Flight.Attribute.DELAY;
import static java.util.Arrays.asList;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static tools.Settings.RowType.JOINED;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import database.Row;

@RunWith(MockitoJUnitRunner.class)
public class AverageTest {

  Average avg = new Average(DELAY);

  @Mock
  Row row;

  @Before
  public void setUp() {
    when(row.get(DELAY.ordinal())).thenReturn(13);
  }

  @Test
  public void shouldSelectValidColumnInMap() {
    assertThat(avg.map(row, null)).isEqualTo(asList("13", "1"));
  }

  @Test
  public void shouldSelectValidColumnForJoinedRow() {
    avg = new Average(DELAY, JOINED);

    assertThat(avg.map(null, row)).isEqualTo(asList("13", "1"));
  }

  @Test
  public void shouldReturZeroWhenNoIterateArgumetsGiven() {
    avg.start();

    assertThat(avg.reduce()).isEqualTo(asList("0"));
  }

  @Test
  public void shouldReduceCorrectly() {
    avg.start();
    avg.iterate(asList("13", "1"));
    avg.iterate(asList("7", "1"));
    avg.iterate(asList("3", "2"));

    assertThat(avg.numberOfIterateArguments()).isEqualTo(2);
    assertThat(avg.reduce()).isEqualTo(asList("5.75"));
  }

}
