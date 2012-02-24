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
    assertThat(flight.getString(UNIQUE_CARRIER)).isEqualTo("B6");
    assertThat(flight.getInt(FLIGHT_NUMBER)).isEqualTo(739);
    assertThat(flight.getInt(ACTUAL_ELAPSED_TIME)).isEqualTo(226);
    assertThat(flight.getString(ORIGIN)).isEqualTo("JFK");
    assertThat(flight.getString(DESTINATION)).isEqualTo("PSE");
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
    boolean result = flight.readFromLine("2008,10,31,5,2359,356,B6,739,226,JFK,PSE,1617,0,-13, 89");
    assertThat(result).isFalse();
  }

  @Test
  public void shouldCheckIfMonthIsInvalid() {
    boolean result = flight.readFromLine("2008,10,hfsdf,5,2359,356,B6,739,226,JFK,PSE,1617,0,-13");
    assertThat(result).isFalse();

    flight.readFromLine("2008,10,0,5,2359,356,B6,739,226,JFK,PSE,1617,0,-13");
    assertThat(flight.isValid()).isFalse();
  }

  @Test
  public void assertThatDivertedCanBeSetToTrue() {
    flight.readFromLine("2008,10,31,5,2359,356,B6,739,226,JFK,PSE,1617,1,-13");
    assertThat(flight.getBoolean(DIVERTED)).isTrue();
  }

}
