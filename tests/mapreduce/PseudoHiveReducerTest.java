package mapreduce;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import operators.ExtendedOperator;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PseudoHiveReducerTest {
  private static final StringArrayWritable KEY = new StringArrayWritable(asList("KEY"));

  @Mock
  ExtendedOperator constantA, constantB;
  @Mock
  Context context;

  PseudoHiveReducer reducer = new PseudoHiveReducer();

  @Before
  public void setUp() {
    setUpExtendedOperator(constantA, 2, "A");
    setUpExtendedOperator(constantB, 1, "B");
    reducer.selectOperators = asList(constantA, constantB);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    reduce(asList(asList("A11", "A21", "B11"), asList("A21", "A22", "B21")));

    verifyOperatorUsage(constantA, asList(asList("A11", "A21"), asList("A21", "A22")));
    verifyOperatorUsage(constantB, asList(asList("B11"), asList("B21")));
    verifyWriteOnContext(asList("A", "B"));
  }

  protected void verifyWriteOnContext(List<String> ls) throws IOException, InterruptedException {
    verify(context).write(KEY, new StringArrayWritable(ls));
  }

  protected void reduce(List<List<String>> values) throws IOException, InterruptedException {
    List<StringArrayWritable> writables = new LinkedList<StringArrayWritable>();
    for (List<String> value : values) {
      writables.add(new StringArrayWritable(value));
    }
    reducer.reduce(KEY, writables, context);
  }

  protected void verifyOperatorUsage(ExtendedOperator operator, List<List<String>> arguments) {
    InOrder inOrder = inOrder(operator);
    inOrder.verify(operator).start();
    for (List<String> ls : arguments) {
      inOrder.verify(operator).iterate(ls);
    }
    inOrder.verify(operator).reduce();
  }

  protected void setUpExtendedOperator(ExtendedOperator operator, int requiredArguments, String returnValue) {
    when(operator.numberOfIterateArguments()).thenReturn(requiredArguments);
    when(operator.reduce()).thenReturn(asList(returnValue));
  }
}
