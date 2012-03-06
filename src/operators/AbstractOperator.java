/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package operators;

import static java.util.Arrays.asList;
import static tools.Settings.RowType.MAIN;

import java.util.List;

import tools.Settings;
import database.Row;

public abstract class AbstractOperator implements Operator {

  protected int columnId;
  protected Settings.RowType rowType;

  public AbstractOperator(Enum<?> enumeration) {
    this(enumeration, MAIN);
  }

  public AbstractOperator(Enum<?> enumeration, Settings.RowType type) {
    columnId = enumeration.ordinal();
    rowType = type;
  }

  @Override
  public List<String> map(Row main, Row joined) {
    Row row = rowType.equals(MAIN) ? main : joined;
    return asList(row.get(columnId).toString());
  }
}