package mapreduce;

import static java.util.Arrays.asList;
import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StringArrayWritableTest {

  @Test
  public void shouldPersistEmptyList() throws IOException {
    shouldPersistTextList(Collections.EMPTY_LIST);
  }

  @Test
  public void shouldPersistNonEmptyList() throws IOException {
    shouldPersistTextList(asList("ala", "has", "cat", "and", "cat", "has", "syphilis"));
  }

  @Test
  public void shouldCompareTwoWritables() throws IOException {
    assertThat(compare(asList("aa", "bb"), asList("aa", "bc"))).isLessThan(0);
    assertThat(compare(asList("aa", "bb", "zzz"), asList("aa", "bc"))).isLessThan(0);
    assertThat(compare(asList("aa", "bb"), asList("aa", "bb"))).isZero();
    assertThat(compare(asList("aa"), asList("aa", "aa"))).isLessThan(0);
    assertThat(compare(asList("aa", "aa"), asList("aa"))).isGreaterThan(0);
    assertThat(compare(asList("bd"), asList("ba", "aa"))).isGreaterThan(0);
  }

  @Test
  public void shouldGenerateTheSameHashCode() {
    List<String> ls = asList("soup", "is", "salty");
    assertThat(compareHashes(ls, ls)).isZero();
  }

  @Test
  public void shouldGenerateDifferentHashCode() {
    assertThat(compareHashes(asList("mushroom", "soup"), asList("mushroom", "pie"))).isNotEqualTo(0);
  }

  private int compareHashes(List<String> a, List<String> b) {
    Integer left = new StringArrayWritable(a).hashCode();
    Integer right = new StringArrayWritable(b).hashCode();
    return left.compareTo(right);
  }

  private int compare(List<String> a, List<String> b) {
    return new StringArrayWritable(a).compareTo(new StringArrayWritable(b));
  }

  private void shouldPersistTextList(List<String> ls) throws IOException {
    StringArrayWritable first = new StringArrayWritable(ls);

    DataOutputBuffer out = new DataOutputBuffer();
    first.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());

    StringArrayWritable second = new StringArrayWritable();
    second.readFields(in);

    assertThat(second.iterator()).containsOnly(ls.toArray());
  }
}
