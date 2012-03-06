/*
 * Pastuszka Przemyslaw
 * University of Wroclaw, Poland
 * 2012
 */
package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringArrayWritable implements WritableComparable<StringArrayWritable>, Iterable<String> {

  private List<String> texts = new LinkedList<String>();

  public StringArrayWritable() {}

  public StringArrayWritable(List<String> ls) {
    texts.addAll(ls);
  }

  public void add(String text) {
    texts.add(text);
  }

  public void addAll(List<String> ls) {
    texts.addAll(ls);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    texts.clear();
    int n = in.readInt();
    for (int i = 0; i < n; ++i) {
      texts.add(Text.readString(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(texts.size());
    for (String text : texts) {
      Text.writeString(out, text);
    }
  }

  @Override
  public int compareTo(StringArrayWritable o) {
    Iterator<String> left = texts.iterator(), right = o.texts.iterator();
    while (left.hasNext() && right.hasNext()) {
      int comparision = left.next().compareTo(right.next());
      if (comparision != 0) {
        return comparision;
      }
    }
    return Integer.compare(texts.size(), o.texts.size());
  }

  @Override
  public Iterator<String> iterator() {
    return texts.iterator();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StringArrayWritable) {
      return this.compareTo((StringArrayWritable) other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return texts.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (Iterator<String> it = texts.iterator(); it.hasNext();) {
      builder.append(it.next());
      if (it.hasNext()) {
        builder.append("\t");
      }
    }
    return builder.toString();
  }

}
