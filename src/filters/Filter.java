/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package filters;

import database.Row;

public interface Filter {
  boolean filter(Row main, Row joined);
}
