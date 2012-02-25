package operators;

import java.util.List;

import org.apache.hadoop.io.Writable;

import database.Row;

public interface Operator {
  List<Writable> map(Row main, Row joined);
}
