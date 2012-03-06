package filters;

import database.Row;

public interface Filter {
  boolean filter(Row main, Row joined);
}
