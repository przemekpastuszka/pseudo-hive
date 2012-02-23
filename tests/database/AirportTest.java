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
    airport.readFromLine("\"0B1\",\"Col. Dyke \",\"Bethel\",\"ME\",\"USA\",44.42506444,-70.80784778");
    assertAirportEqualsTo(airport, "0B1", "Col. Dyke", "Bethel", "ME", "USA", 44.42506444, -70.80784778);
  }

  @Test
  public void shouldParseLineWithEscapeChar() {
    airport.readFromLine("\"DBN\",\"W. H. \\\"Bud\\\" Barron \",\"Dublin\",\"GA\",\"USA\",32.56445806,-82.98525556");
    assertAirportEqualsTo(airport, "DBN", "W. H. \"Bud\" Barron", "Dublin", "GA", "USA", 32.56445806, -82.98525556);
  }

  private void assertAirportEqualsTo(Airport airport, String id, String name, String city, String state, String country, double latitude,
      double longitude) {
    assertThat(airport.isValid()).isTrue();
    assertThat(airport.get(ID)).isEqualTo(id);
    assertThat(airport.get(NAME)).isEqualTo(name);
    assertThat(airport.get(CITY)).isEqualTo(city);
    assertThat(airport.get(STATE)).isEqualTo(state);
    assertThat(airport.get(COUNTRY)).isEqualTo(country);
    assertThat(airport.getDouble(LATITUDE)).isEqualTo(latitude);
    assertThat(airport.getDouble(LONGITUDE)).isEqualTo(longitude);
  }
}
