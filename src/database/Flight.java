/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package database;

import static database.Flight.Attribute.ACTUAL_ELAPSED_TIME;
import static database.Flight.Attribute.ARRIVAL_TIME;
import static database.Flight.Attribute.DAY_OF_MONTH;
import static database.Flight.Attribute.DAY_OF_WEEK;
import static database.Flight.Attribute.DEPARTURE_TIME;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.MONTH;
import static database.Row.Datatype.BOOL;
import static database.Row.Datatype.INT;
import static database.Row.Datatype.TEXT;
import static tools.MathTools.isInRange;

public class Flight extends Row {

  public enum Attribute {
    YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK,
    DEPARTURE_TIME, ARRIVAL_TIME, UNIQUE_CARRIER,
    FLIGHT_NUMBER, ACTUAL_ELAPSED_TIME, ORIGIN,
    DESTINATION, DISTANCE, DIVERTED, DELAY
  }

  @Override
  public Datatype[] getSchema() {
    return new Datatype[] {
        INT, INT, INT, INT,
        INT, INT, TEXT,
        INT, INT, TEXT,
        TEXT, INT, BOOL, INT
    };
  }

  @Override
  public boolean isValid() {
    return fields.size() == 14
        && isInRange(getInt(MONTH), 1, 12)
        && isInRange(getInt(DAY_OF_MONTH), 1, 31)
        && isInRange(getInt(DAY_OF_WEEK), 1, 7)
        && getInt(ACTUAL_ELAPSED_TIME) > 0
        && isInRange(getInt(DISTANCE), 0, 40000)
        && isTimeValid(DEPARTURE_TIME)
        && isTimeValid(ARRIVAL_TIME);
  }

  private boolean isTimeValid(Attribute position) {
    int time = getInt(position);
    return isInRange(getHour(time), 0, 23)
        && isInRange(getMinutes(time), 0, 59);
  }

  private int getHour(int time) {
    return time / 100;
  }

  private int getMinutes(int time) {
    return time % 100;
  }
}
