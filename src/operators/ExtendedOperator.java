/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package operators;

import java.util.List;

public interface ExtendedOperator extends Operator {
  int numberOfIterateArguments();

  void start();

  void iterate(List<String> ls);

  List<String> reduce();

  List<String> combine();
}
