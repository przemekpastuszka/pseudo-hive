package operators;

import java.util.List;

import database.Row;

public interface Operator {
  List<String> map(Row main, Row joined);
}
