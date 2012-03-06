package operators;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static tools.Settings.RowType.MAIN;

import java.util.List;

import tools.Settings.RowType;
import database.Row;

public class Average extends AbstractOperator implements ExtendedOperator {
  private double sum;
  private int count;

  public Average(Enum<?> enumeration) {
    super(enumeration);
  }

  public Average(Enum<?> enumeration, RowType type) {
    super(enumeration, type);
  }

  @Override
  public List<String> map(Row main, Row joined) {
    Row row = rowType.equals(MAIN) ? main : joined;
    return asList(row.get(columnId).toString(), "1");
  }

  @Override
  public int numberOfIterateArguments() {
    return 2;
  }

  @Override
  public void start() {
    sum = 0;
    count = 0;
  }

  @Override
  public void iterate(List<String> ls) {
    sum += parseDouble(ls.get(0));
    count += parseInt(ls.get(1));
  }

  @Override
  public List<String> reduce() {
    if (count > 0) {
      return asList(Double.toString(sum / count));
    }
    return asList("0");
  }

  @Override
  public List<String> combine() {
    return asList(Double.toString(sum), Integer.toString(count));
  }

}
