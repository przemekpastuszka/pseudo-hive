/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package mapreduce;

import java.util.List;

import operators.ExtendedOperator;

public class PseudoHiveCombiner extends AbstractPseudoHiveReducerOrCombiner {
  @Override
  protected List<String> fetchComputedValue(ExtendedOperator operator) {
    return operator.combine();
  }
}
