package database;

import static database.Flight.Attribute.ACTUAL_ELAPSED_TIME;
import static database.Flight.Attribute.ARRIVAL_TIME;
import static database.Flight.Attribute.DAY_OF_MONTH;
import static database.Flight.Attribute.DAY_OF_WEEK;
import static database.Flight.Attribute.DEPARTURE_TIME;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.MONTH;
import static tools.MathTools.isInRange;

public class Flight extends Row {

  public enum Attribute {
    YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK,
    DEPARTURE_TIME, ARRIVAL_TIME, UNIQUE_CARRIER,
    FLIGHT_NUMBER, ACTUAL_ELAPSED_TIME, ORIGIN,
    DESTINATION, DISTANCE, DIVERTED, DELAY
  }

  @Override
  public boolean isValid() {
    try {
      return fields.length == 14
          && isInRange(getInt(MONTH), 1, 12)
          && isInRange(getInt(DAY_OF_MONTH), 1, 31)
          && isInRange(getInt(DAY_OF_WEEK), 1, 7)
          && getInt(ACTUAL_ELAPSED_TIME) > 0
          && isInRange(getInt(DISTANCE), 0, 40000)
          && isTimeValid(DEPARTURE_TIME)
          && isTimeValid(ARRIVAL_TIME);
    } catch (NumberFormatException e) {
      return false;
    }
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
