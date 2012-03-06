package main;

import static database.Flight.Attribute.DELAY;
import static database.Flight.Attribute.DISTANCE;
import static database.Flight.Attribute.ORIGIN;
import static filters.SimpleFilter.FilterType.GREATER;
import static java.util.Arrays.asList;
import static tools.Settings.RowType.JOINED;
import operators.Average;
import operators.Selector;
import operators.Sum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import database.Airport;
import database.Flight;
import filters.SimpleFilter;

public class Tester extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Tester(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    PseudoHive hive = new PseudoHive();
    hive.setMainInput(new Path(args[0]));
    hive.setOutput(new Path(args[1]));

    hive.setSelectOperators(asList(new Sum(DISTANCE), new Average(DELAY))); // SELECT ... sum(f.distance), avg(f.delay) ...
    hive.setMainRowClass(Flight.class); // FROM flights f, ...

    hive.setJoinInput(new Path(args[2]));
    hive.setJoinedRowClass(Airport.class); // airports a, ...
    hive.setJoinKeys(ORIGIN, Airport.Attribute.ID); // JOIN ON f.origin = a.id

    hive.setFilters(asList(new SimpleFilter(DELAY, GREATER, "10"))); // WHERE f.delay > 10
    hive.setGroupByOperators(asList( // GROUP BY f.origin, a.city
        new Selector(ORIGIN),
        new Selector(Airport.Attribute.CITY, JOINED)));

    return hive.run(getConf());
  }
}
