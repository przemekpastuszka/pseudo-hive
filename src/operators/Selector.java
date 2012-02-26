package operators;


public class Selector extends AbstractOperator implements Operator {

  public Selector(Enum<?> enumeration) {
    super(enumeration);
  }

  public Selector(Enum<?> enumeration, RowType type) {
    super(enumeration, type);
  }
}
