package org.rmow.bench.oracle;

import org.apache.iceberg.data.Record;
import org.rmow.bench.workload.Rows;

/**
 * An order-independent digest of a set of rows: the row count and the wrapping sum of each row's
 * {@link Rows#rowHash}. Two reads agree when they return the same (id, version) multiset with
 * overwhelming probability; a duplicated or missing row changes the count.
 */
public final class TableHash {

  private long count = 0;
  private long sum = 0;

  public TableHash() {}

  public TableHash(long count, long sum) {
    this.count = count;
    this.sum = sum;
  }

  public void add(long id, int version) {
    count += 1;
    sum += Rows.rowHash(id, version);
  }

  /**
   * Counts {@code n} rows without hashing them: a measured read's, whose rows only the count
   * checks (the hash's sum stays 0, so only {@link #count} compares).
   */
  public void addCount(long n) {
    count += n;
  }

  public void remove(long id, int version) {
    count -= 1;
    sum -= Rows.rowHash(id, version);
  }

  /** Adds a decoded row, checking its payload when {@code verify} is set. */
  public void add(Record r, boolean verify) {
    if (verify) {
      Rows.verify(r);
    }
    add((Long) r.get(Rows.ID), (Integer) r.get(Rows.VERSION));
  }

  public void addAll(TableHash other) {
    count += other.count;
    sum += other.sum;
  }

  public long count() {
    return count;
  }

  public long sum() {
    return sum;
  }

  public TableHash copy() {
    return new TableHash(count, sum);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TableHash t && t.count == count && t.sum == sum;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(sum) * 31 + Long.hashCode(count);
  }

  @Override
  public String toString() {
    return "TableHash{count=" + count + ", sum=" + Long.toHexString(sum) + "}";
  }
}
