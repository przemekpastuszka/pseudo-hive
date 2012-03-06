package tools;

public class Settings {

  public enum RowType {
    MAIN, JOINED
  }

  public static final String SELECT_OPERATORS = "select_operators";
  public static final String GROUP_BY_OPERATORS = "group_by_operators";
  public static final String MAIN_TABLE_ROW_CLASS_NAME = "main_table_row_class_name";
  public static final String MAIN_TABLE_JOIN_KEY = "main_table_join_key";
  public static final String USE_JOIN_TABLE = "use_join_table";
  public static final String JOINED_TABLE_HASH_MAP = "joined_table_hash_map";
  public static final String FILTERS = "filters";
}
