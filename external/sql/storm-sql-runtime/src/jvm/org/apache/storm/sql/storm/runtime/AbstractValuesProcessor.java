package org.apache.storm.sql.storm.runtime;

import backtype.storm.tuple.Values;

import java.util.Iterator;
import java.util.Map;

/**
 * Subclass of AbstractTupleProcessor provides a series of tuple. It
 * takes a series of iterators of {@link Values} and produces a stream of
 * tuple.
 *
 * The subclass implements the {@see next()} method to provide
 * the output of the stream. It can choose to return null in {@see next()} to
 * indicate that this particular iteration is a no-op. SQL processors depend
 * on this semantic to implement filtering and nullable records.
 */
public abstract class AbstractValuesProcessor {

  /**
   * @return the next tuple in the output stream, null if the tuple should be
   * skipped.
   */
  public abstract Values next();

  /**
   * Initialize the data sources.
   *
   * @param data a map from the table name to the iterators of the values.
   *
   */
  public abstract void initialize(Map<String, Iterator<Values>> data);

  protected abstract Iterator<Values>[] getDataSource();
  /**
   * Does the output stream potentially has more output.
   */
  public boolean mayHasNext() {
    Iterator<Values>[] d = getDataSource();
    for (int i = 0; i < d.length; ++i) {
      if (d[i].hasNext()) {
        return true;
      }
    }
    return false;
  }
}
