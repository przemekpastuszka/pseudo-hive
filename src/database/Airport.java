package database;

import static database.Airport.Attribute.LATITUDE;
import static database.Airport.Attribute.LONGITUDE;
import static tools.MathTools.isInRange;

public class Airport extends Row {

  public enum Attribute {
    ID, NAME, CITY, STATE, COUNTRY, LATITUDE, LONGITUDE
  }

  @Override
  public boolean isValid() {
    try {
      return fields.length == 7
          && isInRange(getDouble(LATITUDE), 0, 90)
          && isInRange(getDouble(LONGITUDE), -180, 180);
    } catch (NumberFormatException e) {
      return false;
    }
  }
}