package org.rmow.bench.bench;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

/**
 * The calling thread's user-mode instructions and cycles, from the CPU's performance counters
 * ({@code perf_event_open}). Thread CPU time moves with the clock speed: the same read repeated
 * back to back took up to twice the CPU time on a laptop, while its instructions stayed within a
 * few in a million. Cycles do not move with the clock, only with what the core shares (an SMT
 * sibling, the caches). The counters need {@code kernel.perf_event_paranoid} at most 2 and, in a
 * container, a seccomp profile that allows the call ({@code CAP_PERFMON}); without them {@link
 * #now} returns null, and no record carries counts.
 */
public final class ThreadCounters {

  /** Counts so far on one thread. */
  public record Sample(long instructions, long cycles) {
    public Sample minus(Sample o) {
      return new Sample(instructions - o.instructions, cycles - o.cycles);
    }
  }

  private interface LibC extends Library {
    NativeLong syscall(NativeLong number, Object... args);

    NativeLong read(int fd, Pointer buf, NativeLong count);
  }

  private static final int PERF_TYPE_HARDWARE = 0;
  private static final long CYCLES = 0;
  private static final long INSTRUCTIONS = 1;
  private static final long PERF_FORMAT_GROUP = 1 << 3;
  private static final long EXCLUDE_KERNEL = 1 << 5;
  private static final long EXCLUDE_HV = 1 << 6;

  private static final LibC LIBC;
  private static final long SYS_PERF_EVENT_OPEN;
  private static final String UNAVAILABLE;

  static {
    LibC libc = null;
    String why = null;
    long nr = switch (System.getProperty("os.arch")) {
      case "amd64", "x86_64" -> 298;
      case "aarch64" -> 241;
      default -> -1;
    };
    if (nr < 0 || !System.getProperty("os.name").equals("Linux")) {
      why = "not Linux on x86-64 or aarch64";
    } else {
      try {
        libc = Native.load("c", LibC.class);
      } catch (UnsatisfiedLinkError | RuntimeException e) {
        why = "no libc through JNA: " + e.getMessage();
      }
    }
    LIBC = libc;
    SYS_PERF_EVENT_OPEN = nr;
    String probe = why;
    if (probe == null) {
      try {
        open();
      } catch (IllegalStateException e) {
        probe = e.getMessage();
      }
    }
    UNAVAILABLE = probe;
  }

  /** This thread's group leader (cycles; instructions follow it), opened on first use. */
  private static final ThreadLocal<Integer> LEADER =
      ThreadLocal.withInitial(() -> UNAVAILABLE == null ? open() : -1);

  private ThreadCounters() {}

  /** Null when the counters can be read; otherwise why not. */
  public static String unavailable() {
    return UNAVAILABLE;
  }

  /** The calling thread's counts so far, or null without counters. */
  public static Sample now() {
    int fd = LEADER.get();
    if (fd < 0) {
      return null;
    }
    try (Memory buf = new Memory(24)) { // nr, then the leader's value, then the member's
      long n = LIBC.read(fd, buf, new NativeLong(24)).longValue();
      if (n != 24 || buf.getLong(0) != 2) {
        throw new IllegalStateException("Short read of the performance counters: " + n);
      }
      return new Sample(buf.getLong(16), buf.getLong(8));
    }
  }

  /** Opens the calling thread's counters, counting from now; returns the group leader. */
  private static int open() {
    int leader = event(CYCLES, -1);
    event(INSTRUCTIONS, leader);
    return leader;
  }

  private static int event(long config, int group) {
    try (Memory attr = new Memory(128)) {
      attr.clear();
      attr.setInt(0, PERF_TYPE_HARDWARE);
      attr.setInt(4, 128); // size
      attr.setLong(8, config);
      attr.setLong(32, PERF_FORMAT_GROUP); // read_format
      attr.setLong(40, EXCLUDE_KERNEL | EXCLUDE_HV); // enabled, user mode only
      // every argument a long: varargs would not sign-extend an int's -1
      long fd =
          LIBC.syscall(new NativeLong(SYS_PERF_EVENT_OPEN), attr, new NativeLong(0),
                  new NativeLong(-1), new NativeLong(group), new NativeLong(0))
              .longValue();
      if (fd < 0) {
        throw new IllegalStateException(
            "perf_event_open failed (errno " + Native.getLastError()
                + "): kernel.perf_event_paranoid above 2, or a container without CAP_PERFMON");
      }
      return (int) fd;
    }
  }
}
