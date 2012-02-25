package database;

import static database.Airport.Attribute.LATITUDE;
import static database.Airport.Attribute.LONGITUDE;
import static database.Row.Datatype.DOUBLE;
import static database.Row.Datatype.TEXT;
import static tools.MathTools.isInRange;

public class Airport extends Row {

  public enum Attribute {
    ID, NAME, CITY, STATE, COUNTRY, LATITUDE, LONGITUDE
  }

  @Override
  public Datatype[] getSchema() {
    return new Datatype[] {
        TEXT, TEXT, TEXT, TEXT, TEXT, DOUBLE, DOUBLE
    };
  }

  @Override
  public boolean isValid() {
    return fields.size() == 7
        && isInRange(getDouble(LATITUDE), 0, 90)
        && isInRange(getDouble(LONGITUDE), -180, 180);
  }

  public Airport() {}

  public Airport(String line) {
    readFromLine(line);
  }
}