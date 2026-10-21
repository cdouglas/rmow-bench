package org.rmow.bench.arms;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.SeekableInputStream;
import org.rmow.bench.io.RelocatedFileIO;
import org.rmow.bench.oracle.TableHash;
import org.rmow.bench.rmow.LogicalFile;
import org.rmow.bench.rmow.VersionDigest;

/**
 * Dumps every version of every appendable object the RMoW arms wrote, so other Parquet readers can
 * read each footer the arms actually wrote ({@code analysis/interop_check.py}).
 *
 * <ul>
 *   <li>{@code <dir>/<arm>/<object>}: every object, a hard link into the local store (its bytes
 *       no longer change once the run ends) or a copy of an in-memory one. A version of an
 *       appendable object is its prefix; a split file's versions are objects of their own.
 *   <li>{@code <dir>/versions.csv}: one row per version, with what Iceberg's reader decodes from
 *       that prefix ({@link VersionDigest}).
 * </ul>
 *
 * Also checks each step against the reference model, for arms without DVs: the file versions the
 * table held after a step must add up to the model's digest then. Probes read the table only every
 * few steps; this covers every version any step held.
 */
public final class VersionDump {

  private VersionDump() {}

  /** Returns the number of versions written. */
  public static int write(
      Path dir,
      String storeType,
      RelocatedFileIO io,
      List<Arm> arms,
      IntFunction<TableHash> expectedAfter)
      throws IOException {
    Files.createDirectories(dir);
    int count = 0;
    try (BufferedWriter csv = Files.newBufferedWriter(dir.resolve("versions.csv"))) {
      csv.write(VersionDigest.CSV_HEADER);
      csv.newLine();
      for (Arm arm : arms) {
        if (arm instanceof RmowArm rmow) {
          count += write(dir, storeType, io, rmow, expectedAfter, csv);
        }
      }
    }
    return count;
  }

  private static int write(
      Path dir,
      String storeType,
      RelocatedFileIO io,
      RmowArm arm,
      IntFunction<TableHash> expectedAfter,
      BufferedWriter csv)
      throws IOException {
    String root = arm.ctx().root() + "/";
    Map<String, VersionDigest> digests = new HashMap<>(); // by location@length
    int count = 0;
    for (LogicalFile f : arm.objects()) {
      for (String object : f.objects()) {
        Path target = dir.resolve(arm.name()).resolve(relative(root, object));
        Files.createDirectories(target.getParent());
        link(storeType, io, object, target);
      }
      List<LogicalFile.Version> versions = f.versions();
      for (int v = 0; v < versions.size(); v++) {
        LogicalFile.Version at = versions.get(v);
        VersionDigest d = VersionDigest.read(io, at.location(), at.length());
        digests.put(at.location() + "@" + at.length(), d);
        csv.write(d.csv(arm.name(), arm.name() + "/" + relative(root, at.location()), v, at.length()));
        csv.newLine();
        count++;
      }
    }

    if (!arm.dvSealed()) {
      for (int step = 0; step < arm.steps(); step++) {
        TableHash got = new TableHash();
        for (Map.Entry<String, Long> v : arm.versionsAfter(step)) {
          got.addAll(digests.get(v.getKey() + "@" + v.getValue()).hash());
        }
        TableHash want = expectedAfter.apply(step);
        if (!got.equals(want)) {
          throw new IllegalStateException(
              String.format(
                  "%s: the file versions after step %d hold %s, the model %s",
                  arm.name(), step, got, want));
        }
      }
    }
    return count;
  }

  private static String relative(String root, String location) {
    if (!location.startsWith(root)) {
      throw new IllegalStateException(location + " is outside the arm's root " + root);
    }
    return location.substring(root.length());
  }

  private static void link(String storeType, RelocatedFileIO io, String object, Path target)
      throws IOException {
    Files.deleteIfExists(target);
    if (storeType.equals("local")) {
      String loc = io.physical(object);
      Files.createLink(target, Path.of(loc.startsWith("file:") ? loc.substring(5) : loc));
      return;
    }
    long length = io.newInputFile(object).getLength();
    byte[] bytes = new byte[(int) length];
    try (SeekableInputStream in = io.newInputFile(object, length).newStream()) {
      IOUtil.readFully(in, bytes, 0, bytes.length);
    }
    try (OutputStream out = Files.newOutputStream(target)) {
      out.write(bytes);
    }
  }
}
