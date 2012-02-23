package database;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class Row {
  String[] fields;

  public abstract boolean isValid();

  public String get(Object position) {
    if (position instanceof Integer) {
      return fields[(Integer) position];
    }

    if (position instanceof Enum) {
      Enum<?> enumeration = (Enum<?>) position;
      return fields[enumeration.ordinal()];
    }

    return null;
  }

  public int getInt(Object position) {
    return Integer.parseInt(get(position));
  }

  public double getDouble(Object position) {
    return Double.parseDouble(get(position));
  }

  public boolean getBoolean(Object position) {
    return get(position).equals("1");
  }

  public void readFromLine(String line) {
    fields = line.split(",");
    for (int i = 0; i < fields.length; ++i) {
      fields[i] = removeQuotas(fields[i]).trim();
    }
  }

  private String removeQuotas(String field) {
    if (field.startsWith("\"") && field.endsWith("\"")) {
      String unqoutedString = field.substring(1, field.length() - 1);
      return unqoutedString.replaceAll("\\\\\"", "\"");
    }
    return field;
  }

  @Override
  public boolean equals(Object other) {
    return EqualsBuilder.reflectionEquals(this, other);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }
}
