package org.rmow.bench.fork;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.rmow.bench.append.RegionAppender;

/** The fork appender over an in-memory store with the fork's atomic operations. */
class ForkAppendersTest {

  private static final String LOC = "mem://t/file";

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void appendsChainOnTheSnapshotsTheyReturn() throws IOException {
    MemAtomicFileIO io = new MemAtomicFileIO();
    RegionAppender appender = ForkAppenders.atomic(io);
    appender.create(io, LOC, bytes("first|"));
    assertThat(io.lookups).as("a create pins the object's absence without a lookup").isZero();

    long length = appender.append(LOC, 6, bytes("second|"));
    length = appender.append(LOC, length, bytes("third"));

    assertThat(length).isEqualTo(18);
    assertThat(new String(io.bytes(LOC), StandardCharsets.UTF_8)).isEqualTo("first|second|third");
    assertThat(io.lookups).as("appends to the snapshots writes returned").isZero();
  }

  @Test
  void anAppendItDidNotChainLooksTheObjectUp() throws IOException {
    MemAtomicFileIO io = new MemAtomicFileIO();
    ForkAppenders.atomic(io).create(io, LOC, bytes("first|"));
    int before = io.lookups;

    // another appender, as after a restart: it has no snapshot of the object
    long length = ForkAppenders.atomic(io).append(LOC, 6, bytes("second"));

    assertThat(length).isEqualTo(12);
    assertThat(io.lookups - before).isEqualTo(1);
  }

  @Test
  void appendsOnlyAtTheObjectsLength() throws IOException {
    MemAtomicFileIO io = new MemAtomicFileIO();
    RegionAppender appender = ForkAppenders.atomic(io);
    appender.create(io, LOC, bytes("0123456789"));
    for (long wrong : new long[] {0, 9, 11}) {
      assertThatThrownBy(() -> ForkAppenders.atomic(io).append(LOC, wrong, bytes("x")))
          .as("append at " + wrong)
          .isInstanceOf(IOException.class)
          .hasMessageContaining("APPEND");
    }
    assertThat(io.bytes(LOC)).isEqualTo(bytes("0123456789"));
  }

  @Test
  void aStaleSnapshotLosesToAnotherWriter() throws IOException {
    MemAtomicFileIO io = new MemAtomicFileIO();
    RegionAppender mine = ForkAppenders.atomic(io);
    mine.create(io, LOC, bytes("base|"));
    // another writer appends to the same version first
    ForkAppenders.atomic(io).append(LOC, 5, bytes("theirs|"));

    assertThatThrownBy(() -> mine.append(LOC, 5, bytes("mine")))
        .isInstanceOf(IOException.class);
    assertThat(new String(io.bytes(LOC), StandardCharsets.UTF_8)).isEqualTo("base|theirs|");
  }

  @Test
  void aCreateFailsIfTheObjectExists() throws IOException {
    MemAtomicFileIO io = new MemAtomicFileIO();
    RegionAppender appender = ForkAppenders.atomic(io);
    appender.create(io, LOC, bytes("first"));

    assertThatThrownBy(() -> ForkAppenders.atomic(io).create(io, LOC, bytes("again")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CAS");
    assertThat(new String(io.bytes(LOC), StandardCharsets.UTF_8)).isEqualTo("first");
  }
}
