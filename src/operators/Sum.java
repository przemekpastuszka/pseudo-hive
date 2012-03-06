package operators;

import static java.lang.Double.parseDouble;
import static java.util.Arrays.asList;

import java.util.List;

import tools.Settings;

public class Sum extends AbstractOperator implements ExtendedOperator {
  private double sum;

  public Sum(Enum<?> enumeration) {
    super(enumeration);
  }

  public Sum(Enum<?> enumeration, Settings.RowType type) {
    super(enumeration, type);
  }

  @Override
  public int numberOfIterateArguments() {
    return 1;
  }

  @Override
  public void start() {
    sum = 0;
  }

  @Override
  public void iterate(List<String> ls) {
    sum += parseDouble(ls.get(0));
  }

  @Override
  public List<String> reduce() {
    return asList(Double.toString(sum));
  }

  @Override
  public List<String> combine() {
    return reduce();
  }
}
