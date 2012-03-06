package filters;

import static java.lang.Double.parseDouble;
import static org.apache.commons.lang3.BooleanUtils.negate;
import static tools.Settings.RowType.MAIN;
import tools.Settings;
import database.Row;

public class SimpleFilter implements Filter {

  public enum FilterType {
    LESS, GREATER, EQUAL, NOT_EQUAL
  };

  protected int columnId;
  protected Settings.RowType rowType;
  protected FilterType filterType;
  protected String value;

  public SimpleFilter(Enum<?> enumeration, FilterType filterType, String value) {
    this(enumeration, MAIN, filterType, value);
  }

  public SimpleFilter(Enum<?> enumeration, Settings.RowType type, FilterType filterType, String value) {
    columnId = enumeration.ordinal();
    rowType = type;
    this.value = value;
    this.filterType = filterType;
  }

  @Override
  public boolean filter(Row main, Row joined) {
    Row row = rowType.equals(MAIN) ? main : joined;
    String valueToCompare = row.get(columnId).toString();

    switch (filterType) {
      case EQUAL:
        return valueToCompare.equals(value);
      case NOT_EQUAL:
        return negate(valueToCompare.equals(value));
      case LESS:
        return parseDouble(valueToCompare) < parseDouble(value);
      case GREATER:
        return parseDouble(valueToCompare) > parseDouble(value);
    }
    return false;
  }
}