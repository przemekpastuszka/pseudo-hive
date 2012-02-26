package operators;

import static java.util.Arrays.asList;
import static operators.AbstractOperator.RowType.MAIN;

import java.util.List;

import database.Row;

public abstract class AbstractOperator implements Operator {

  public enum RowType {
    MAIN, JOINED
  }

  protected int columnId;
  protected RowType rowType;

  public AbstractOperator(Enum<?> enumeration) {
    this(enumeration, MAIN);
  }

  public AbstractOperator(Enum<?> enumeration, RowType type) {
    columnId = enumeration.ordinal();
    rowType = type;
  }

  @Override
  public List<String> map(Row main, Row joined) {
    Row row = rowType.equals(MAIN) ? main : joined;
    return asList(row.get(columnId).toString());
  }
}