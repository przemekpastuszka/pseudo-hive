package database;

import static database.Airport.Attribute.CITY;
import static database.Airport.Attribute.COUNTRY;
import static database.Airport.Attribute.ID;
import static database.Airport.Attribute.LATITUDE;
import static database.Airport.Attribute.LONGITUDE;
import static database.Airport.Attribute.NAME;
import static database.Airport.Attribute.STATE;
import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AirportTest {

  Airport airport = new Airport();

  @Test
  public void shouldParseSimpleLine() {
    assertAirportEqualsTo("\"0B1\",\"Col. Dyke \",\"Bethel\",\"ME\",\"USA\",44.42506444,-70.80784778",
        "0B1", "Col. Dyke", "Bethel", "ME", "USA", 44.42506444, -70.80784778);
  }

  @Test
  public void shouldParseLineWithEscapeChar() {
    assertAirportEqualsTo("\"DBN\",\"W. H. \\\"Bud\\\" Barron \",\"Dublin\",\"GA\",\"USA\",32.56445806,-82.98525556",
        "DBN", "W. H. \"Bud\" Barron", "Dublin", "GA", "USA", 32.56445806, -82.98525556);
  }

  private void assertAirportEqualsTo(String row, String id, String name, String city, String state, String country, double latitude,
      double longitude) {
    assertThat(airport.readFromLine(row)).isTrue();
    assertThat(airport.isValid()).isTrue();
    assertThat(airport.getString(ID)).isEqualTo(id);
    assertThat(airport.getString(NAME)).isEqualTo(name);
    assertThat(airport.getString(CITY)).isEqualTo(city);
    assertThat(airport.getString(STATE)).isEqualTo(state);
    assertThat(airport.getString(COUNTRY)).isEqualTo(country);
    assertThat(airport.getDouble(LATITUDE)).isEqualTo(latitude);
    assertThat(airport.getDouble(LONGITUDE)).isEqualTo(longitude);
  }
}
