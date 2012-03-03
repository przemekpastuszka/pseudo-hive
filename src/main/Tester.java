package main;

import static database.Flight.Attribute.DELAY;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.ORIGIN;
import static java.util.Arrays.asList;
import static operators.AbstractOperator.RowType.JOINED;
import operators.Average;
import operators.Selector;
import operators.Sum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import database.Airport;
import database.Flight;

public class Tester {

  public static void main(String[] args) throws Exception {
    PseudoHive hive = new PseudoHive();
    hive.setMainInput(new Path(args[0]));
    hive.setOutput(new Path(args[1]));
    hive.setMainRowClass(Flight.class);

    hive.setJoinInput(new Path(args[2]));
    hive.setJoinedRowClass(Airport.class);
    hive.setJoinKeys(ORIGIN, Airport.Attribute.ID);

    hive.setGroupByOperators(asList(
        new Selector(ORIGIN),
        new Selector(Airport.Attribute.CITY, JOINED)));
    hive.setSelectOperators(asList(new Sum(DISTANCE), new Average(DELAY)));

    int exitCode = ToolRunner.run(hive, args);
    System.exit(exitCode);
  }
}
