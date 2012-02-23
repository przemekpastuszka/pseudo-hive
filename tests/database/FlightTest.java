package database;

import static database.Flight.Attribute.ACTUAL_ELAPSED_TIME;
import static database.Flight.Attribute.ARRIVAL_TIME;
import static database.Flight.Attribute.DAY_OF_MONTH;
import static database.Flight.Attribute.DAY_OF_WEEK;
import static database.Flight.Attribute.DELAY;
import static database.Flight.Attribute.DEPARTURE_TIME;
import static database.Flight.Attribute.DESTINATION;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.DIVERTED;
import static database.Flight.Attribute.FLIGHT_NUMBER;
import static database.Flight.Attribute.MONTH;
import static database.Flight.Attribute.ORIGIN;
import static database.Flight.Attribute.UNIQUE_CARRIER;
import static database.Flight.Attribute.YEAR;
import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FlightTest {

  Flight flight = new Flight();

  @Before
  public void setUp() {
    flight.readFromLine("2008,10,31,5,2359,356,B6,739,226,JFK,PSE,1617,0,-13");
  }

  @Test
  public void shouldParseSimpleLine() {
    assertThat(flight.isValid()).isTrue();
    assertThat(flight.getInt(YEAR)).isEqualTo(2008);
    assertThat(flight.getInt(MONTH)).isEqualTo(10);
    assertThat(flight.getInt(DAY_OF_MONTH)).isEqualTo(31);
    assertThat(flight.getInt(DAY_OF_WEEK)).isEqualTo(5);
    assertThat(flight.getInt(DEPARTURE_TIME)).isEqualTo(2359);
    assertThat(flight.getInt(ARRIVAL_TIME)).isEqualTo(356);
    assertThat(flight.get(UNIQUE_CARRIER)).isEqualTo("B6");
    assertThat(flight.getInt(FLIGHT_NUMBER)).isEqualTo(739);
    assertThat(flight.getInt(ACTUAL_ELAPSED_TIME)).isEqualTo(226);
    assertThat(flight.get(ORIGIN)).isEqualTo("JFK");
    assertThat(flight.get(DESTINATION)).isEqualTo("PSE");
    assertThat(flight.getInt(DISTANCE)).isEqualTo(1617);
    assertThat(flight.getBoolean(DIVERTED)).isFalse();
    assertThat(flight.getInt(DELAY)).isEqualTo(-13);
  }

  @Test
  public void shouldBeNonValidIfTooFewFieldsGiven() {
    flight.readFromLine("2008,10,31,5,2359,356,B6,739,226");
    assertThat(flight.isValid()).isFalse();
  }

  @Test
  public void shouldBeNonValidIfTooManyFieldsGiven() {
    flight.readFromLine("2008,10,31,5,2359,356,B6,739,226,JFK,PSE,1617,0,-13,89");
    assertThat(flight.isValid()).isFalse();
  }

  @Test
  public void shouldCheckIfMonthIsValid() {
    assertThatIsValidForIntegerSequence(MONTH, 1, 12);

    flight.fields[MONTH.ordinal()] = "hfsdhf";
    assertThat(flight.isValid()).isFalse();
  }

  @Test
  public void shouldCheckIfDayOfMonthIsValid() {
    assertThatIsValidForIntegerSequence(DAY_OF_MONTH, 1, 31);
  }

  @Test
  public void shouldCheckIfDayOfWeekIsValid() {
    assertThatIsValidForIntegerSequence(DAY_OF_WEEK, 1, 7);
  }

  @Test
  public void assertThatDivertedCanBeSetToTrue() {
    flight.fields[DIVERTED.ordinal()] = "1";
    assertThat(flight.getBoolean(DIVERTED)).isTrue();
  }

  private void assertThatIsValidForIntegerSequence(Flight.Attribute field, int begin, int end) {
    int position = field.ordinal();
    for (int i = begin; i <= end; ++i) {
      flight.fields[position] = Integer.toString(i);
      assertThat(flight.isValid()).isTrue();
    }

    flight.fields[position] = Integer.toString(begin - 1);
    assertThat(flight.isValid()).isFalse();

    flight.fields[position] = Integer.toString(end + 1);
    assertThat(flight.isValid()).isFalse();
  }
}
