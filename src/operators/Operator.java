/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package operators;

import java.util.List;

import database.Row;

public interface Operator {
  List<String> map(Row main, Row joined);
}
