package main;

import static database.Flight.Attribute.DELAY;
import static database.Flight.Attribute.ORIGIN;
import static java.util.Arrays.asList;
import operators.Selector;
import operators.Sum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import database.Flight;

public class Tester {

  public static void main(String[] args) throws Exception {
    PseudoHive hive = new PseudoHive();
    hive.setMainInput(new Path(args[0]));
    hive.setOutput(new Path(args[1]));
    hive.setMainRowClass(Flight.class);
    hive.setGroupByOperators(asList(new Selector(ORIGIN)));
    hive.setSelectOperators(asList(new Sum(DELAY)));

    int exitCode = ToolRunner.run(hive, args);
    System.exit(exitCode);
  }
}
