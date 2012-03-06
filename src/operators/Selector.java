package operators;

import tools.Settings;

public class Selector extends AbstractOperator implements Operator {

  public Selector(Enum<?> enumeration) {
    super(enumeration);
  }

  public Selector(Enum<?> enumeration, Settings.RowType type) {
    super(enumeration, type);
  }
}
