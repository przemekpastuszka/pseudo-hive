/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package tools;

import database.Row;

public class RowParser {
  public static void parseValue(String value, Row row) throws MalformedRecordException {
    boolean readSuccess = row.readFromLine(value);
    if (readSuccess) {
      readSuccess = row.isValid();
    }
    if (readSuccess == false) {
      throw new MalformedRecordException();
    }
  }

  public static Row instantiateNewRow(Class<? extends Row> clazz) {
    try {
      return clazz.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static class MalformedRecordException extends Exception {
    private static final long serialVersionUID = 931758288441250318L;
  }
}
