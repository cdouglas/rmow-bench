package org.rmow.bench.workload;

import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.avro.util.Utf8;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

/**
 * The benchmark table's schema and rows. A row is a pure function of (id, ts, version), so op logs
 * never store payloads and any reader can check a row it decoded.
 *
 * <p>The payload mixes column shapes on purpose: a low-cardinality long (RLE/dictionary), a long
 * that grows with the id (delta-friendly), a random long and double (incompressible), a version
 * stamp, and two dictionary-friendly strings.
 */
public final class Rows {

  public static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "ts", Types.LongType.get()),
          required(3, "version", Types.IntegerType.get()),
          required(4, "l1", Types.LongType.get()),
          required(5, "l2", Types.LongType.get()),
          required(6, "l3", Types.LongType.get()),
          required(7, "l4", Types.LongType.get()),
          required(8, "d1", Types.DoubleType.get()),
          required(9, "d2", Types.DoubleType.get()),
          required(10, "s1", Types.StringType.get()),
          required(11, "s2", Types.StringType.get()));

  public static final int ID = 0;
  public static final int TS = 1;
  public static final int VERSION = 2;

  // UTF-8 bytes, as Spark's rows hold strings (UTF8String): Iceberg's writer writes a Utf8's bytes
  // as they are, where it would encode a String's again for every value
  private static final Utf8[] S1 = vocab("category-", 64);
  private static final Utf8[] S2 = vocab("region-", 1024);

  private Rows() {}

  private static Utf8[] vocab(String prefix, int n) {
    Utf8[] v = new Utf8[n];
    for (int i = 0; i < n; i++) {
      v[i] = new Utf8(prefix + String.format("%04d", i));
    }
    return v;
  }

  /** The splitmix64 finalizer: a fast, well-mixed 64-bit hash. */
  public static long mix64(long z) {
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  private static long h(long id, int version, int field) {
    return mix64(id * 0x9E3779B97F4A7C15L + version * 0xD1B54A32D192ED03L + field);
  }

  public static Record record(long id, long ts, int version) {
    GenericRecord r = GenericRecord.create(SCHEMA);
    r.set(0, id);
    r.set(1, ts);
    r.set(2, version);
    r.set(3, Math.floorMod(h(id, version, 3), 1000L));
    r.set(4, id * 7 + (h(id, version, 4) & 0xFF));
    r.set(5, h(id, version, 5));
    r.set(6, version * 1_000_000L + Math.floorMod(h(id, version, 6), 1_000_000L));
    r.set(7, Math.floorMod(h(id, version, 7), 100_000L) / 100.0);
    r.set(8, (h(id, version, 8) >>> 11) * 0x1.0p-53);
    r.set(9, S1[(int) Math.floorMod(h(id, version, 9), (long) S1.length)]);
    r.set(10, S2[(int) Math.floorMod(h(id, version, 10), (long) S2.length)]);
    return r;
  }

  /** The hash a row contributes to a {@link org.rmow.bench.oracle.TableHash}. */
  public static long rowHash(long id, int version) {
    return mix64(id * 0x9E3779B97F4A7C15L ^ (version + 0x632BE59BD9B4E019L));
  }

  /** A string column's value as a {@code String} (a reader's, or the writer's {@code Utf8}). */
  private static Object text(Object value) {
    return value instanceof CharSequence ? value.toString() : value;
  }

  /** Throws if a decoded row's payload is not the one its (id, ts, version) determines. */
  public static void verify(Record r) {
    long id = (Long) r.get(0);
    long ts = (Long) r.get(1);
    int version = (Integer) r.get(2);
    Record expected = record(id, ts, version);
    for (int i = 3; i < SCHEMA.columns().size(); i++) {
      Object got = r.get(i);
      Object want = expected.get(i);
      if (!text(want).equals(text(got))) {
        throw new IllegalStateException(
            "Row " + id + " v" + version + " column " + i + ": got " + got + ", want " + want);
      }
    }
  }
}
