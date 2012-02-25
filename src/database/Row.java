package database;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class Row implements Iterable<Object> {
  public enum Datatype {
    TEXT, INT, DOUBLE, BOOL
  };

  List<Object> fields;

  public abstract boolean isValid();

  public abstract Datatype[] getSchema();

  public boolean readFromLine(String line) {
    String[] preformattedFields = getPreformattedFields(line);

    try {
      createFields(preformattedFields);
    } catch (NumberFormatException e) {
      return false;
    } catch (ArrayIndexOutOfBoundsException e) {
      return false;
    }
    return true;
  }

  private void createFields(String[] textFields) {
    fields = new ArrayList<Object>(textFields.length);
    for (int i = 0; i < textFields.length; ++i) {
      fields.add(createObject(i, textFields[i]));
    }
  }

  private String[] getPreformattedFields(String line) {
    String[] textFields = line.split(",");
    for (int i = 0; i < textFields.length; ++i) {
      textFields[i] = removeQuotas(textFields[i]).trim();
    }
    return textFields;
  }

  private String removeQuotas(String field) {
    if (field.startsWith("\"") && field.endsWith("\"")) {
      String unqoutedString = field.substring(1, field.length() - 1);
      return unqoutedString.replaceAll("\\\\\"", "\"");
    }
    return field;
  }

  private Object createObject(int i, String textField) {
    switch (getSchema()[i]) {
      case DOUBLE:
        return Double.parseDouble(textField);
      case INT:
        return Integer.parseInt(textField);
      case TEXT:
        return textField;
      case BOOL:
        return textField.equals("1");
    }
    return null;
  }

  public Object get(Object position) {
    if (position instanceof Integer) {
      return fields.get((Integer) position);
    }

    if (position instanceof Enum) {
      Enum<?> enumeration = (Enum<?>) position;
      return fields.get(enumeration.ordinal());
    }

    return null;
  }

  public String getString(Object position) {
    return (String) get(position);
  }

  public int getInt(Object position) {
    return (Integer) get(position);
  }

  public double getDouble(Object position) {
    return (Double) get(position);
  }

  public boolean getBoolean(Object position) {
    return (Boolean) get(position);
  }

  @Override
  public boolean equals(Object other) {
    return EqualsBuilder.reflectionEquals(this, other);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public Iterator<Object> iterator() {
    return fields.iterator();
  }
}
