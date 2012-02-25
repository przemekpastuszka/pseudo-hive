package mapreduce;

import static tools.Settings.SELECT_OPERATORS;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import operators.ExtendedOperator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class PseudoHiveReducer extends Reducer<StringArrayWritable, StringArrayWritable, StringArrayWritable, StringArrayWritable> {

  List<ExtendedOperator> selectOperators;

  @SuppressWarnings("unchecked")
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    XStream xstream = new XStream(new StaxDriver());
    selectOperators = (List<ExtendedOperator>) xstream.fromXML(conf.get(SELECT_OPERATORS));
  }

  @Override
  protected void reduce(StringArrayWritable key, Iterable<StringArrayWritable> values, Context context)
      throws IOException, InterruptedException {

    startOperators();
    iterate(values);

    StringArrayWritable outputValue = reduceToOutputValue();
    context.write(key, outputValue);
  }

  protected void iterate(Iterable<StringArrayWritable> values) {
    for (StringArrayWritable writable : values) {
      Iterator<String> it = writable.iterator();
      for (ExtendedOperator operator : selectOperators) {
        iterateOperator(it, operator);
      }
    }
  }

  protected void iterateOperator(Iterator<String> it, ExtendedOperator operator) {
    int requiredNumberOfArguments = operator.numberOfIterateArguments();
    List<String> ls = takeElements(it, requiredNumberOfArguments);
    operator.iterate(ls);
  }

  protected List<String> takeElements(Iterator<String> it, int k) {
    List<String> ls = new LinkedList<String>();
    for (int i = 0; i < k; ++i) {
      ls.add(it.next());
    }
    return ls;
  }

  protected StringArrayWritable reduceToOutputValue() {
    StringArrayWritable outputValue = new StringArrayWritable();
    for (ExtendedOperator operator : selectOperators) {
      outputValue.addAll(operator.reduce());
    }
    return outputValue;
  }

  protected void startOperators() {
    for (ExtendedOperator operator : selectOperators) {
      operator.start();
    }
  }
}
