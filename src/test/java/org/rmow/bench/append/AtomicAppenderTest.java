package org.rmow.bench.append;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rmow.bench.io.MemFileIO;
import org.rmow.bench.io.PreadFileIO;

/**
 * The appender over the local stores' atomic operations: the in-memory one, which counts the
 * lookups a cloud store would answer with a HEAD, and the local filesystem.
 */
class AtomicAppenderTest {

  private static final String LOC = "mem://t/file";

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String read(SupportsAtomicOperations io, String location) throws IOException {
    InputFile f = io.newInputFile(location);
    byte[] b = new byte[(int) f.getLength()];
    try (SeekableInputStream in = f.newStream()) {
      IOUtil.readFully(in, b, 0, b.length);
    }
    return new String(b, StandardCharsets.UTF_8);
  }

  @Test
  void appendsChainOnTheSnapshotsTheyReturn() throws IOException {
    MemFileIO io = new MemFileIO();
    RegionAppender appender = new AtomicAppender(io);
    appender.create(LOC, bytes("first|"));
    assertThat(io.lookups()).as("a create pins the object's absence without a lookup").isZero();

    long length = appender.append(LOC, 6, bytes("second|"));
    length = appender.append(LOC, length, bytes("third"));

    assertThat(length).isEqualTo(18);
    assertThat(io.lookups()).as("appends to the snapshots writes returned").isZero();
    assertThat(read(io, LOC)).isEqualTo("first|second|third");
  }

  @Test
  void anAppendItDidNotChainLooksTheObjectUp() throws IOException {
    MemFileIO io = new MemFileIO();
    new AtomicAppender(io).create(LOC, bytes("first|"));

    // another appender, as after a restart: it has no snapshot of the object
    long length = new AtomicAppender(io).append(LOC, 6, bytes("second"));

    assertThat(length).isEqualTo(12);
    assertThat(io.lookups()).isEqualTo(1);
  }

  @Test
  void appendsOnlyAtTheObjectsLength() throws IOException {
    MemFileIO io = new MemFileIO();
    new AtomicAppender(io).create(LOC, bytes("0123456789"));
    for (long wrong : new long[] {0, 9, 11}) {
      assertThatThrownBy(() -> new AtomicAppender(io).append(LOC, wrong, bytes("x")))
          .as("append at " + wrong)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("APPEND");
    }
    assertThat(read(io, LOC)).isEqualTo("0123456789");
  }

  @Test
  void aStaleSnapshotLosesToAnotherWriter() throws IOException {
    MemFileIO io = new MemFileIO();
    RegionAppender mine = new AtomicAppender(io);
    mine.create(LOC, bytes("base|"));
    // another writer appends to the same version first
    new AtomicAppender(io).append(LOC, 5, bytes("theirs|"));

    assertThatThrownBy(() -> mine.append(LOC, 5, bytes("mine"))).isInstanceOf(IOException.class);
    assertThat(read(io, LOC)).isEqualTo("base|theirs|");
  }

  @Test
  void aCreateFailsIfTheObjectExists() throws IOException {
    MemFileIO io = new MemFileIO();
    new AtomicAppender(io).create(LOC, bytes("first"));

    assertThatThrownBy(() -> new AtomicAppender(io).create(LOC, bytes("again")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CAS");
    assertThat(read(io, LOC)).isEqualTo("first");
  }

  @Test
  void onALocalFilesystem(@TempDir Path dir) throws IOException {
    PreadFileIO io = new PreadFileIO(false);
    String loc = dir.resolve("a/file").toString();
    RegionAppender appender = new AtomicAppender(io);
    appender.create(loc, bytes("first|"));
    long length = appender.append(loc, 6, bytes("second|"));
    assertThat(appender.append(loc, length, bytes("third"))).isEqualTo(18);
    assertThat(read(io, loc)).isEqualTo("first|second|third");

    assertThatThrownBy(() -> new AtomicAppender(io).append(loc, 6, bytes("x")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("APPEND");
    assertThatThrownBy(() -> new AtomicAppender(io).create(loc, bytes("again")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CAS");
    assertThat(read(io, loc)).isEqualTo("first|second|third");
    try (var files = Files.list(dir.resolve("a"))) {
      assertThat(files).as("no temporary files left").hasSize(1);
    }
  }
}
