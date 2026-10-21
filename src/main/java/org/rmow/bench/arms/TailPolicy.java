package org.rmow.bench.arms;

import java.util.ArrayList;
import java.util.List;

/**
 * Decides which open tail units a batch's inserts merge with. A unit is a row group (RMoW) or a
 * small data file (TailCoW); units are kept on a stack in insertion order, each with a level and a
 * size. A unit stays open until it is too large to merge again (see {@link #sealed}); sealed units
 * leave the stack.
 *
 * <ul>
 *   <li>{@code A} (append): never merge; each batch's inserts are a new unit.
 *   <li>{@code B} (trailing merge, leveling): merge the inserts into the one open unit until it is
 *       sealed. Each batch rewrites the whole open tail, so write amplification is about {@code
 *       sealBytes / (2 * batch bytes)}.
 *   <li>{@code C} (tiered): the inserts are a new level-0 unit; whenever the newest {@code fanout}
 *       units share a level, they merge into one unit a level up, cascading, unless the result
 *       would exceed {@code sealBytes}. Each row is rewritten about {@code log_fanout(sealBytes /
 *       batch bytes)} times.
 * </ul>
 */
public final class TailPolicy {

  public enum Kind {
    A,
    B,
    C
  }

  /** An open unit: its id (row group or file id), level and size in bytes. */
  public record Unit(long id, int level, long bytes) {}

  /** The units to merge with the inserts, and the merged unit's level. */
  public record Plan(List<Unit> merge, int level) {}

  private final Kind kind;
  private final long sealBytes;
  private final int fanout;
  private final List<Unit> stack = new ArrayList<>();

  public TailPolicy(Kind kind, long sealBytes, int fanout) {
    if (kind == Kind.C && fanout < 2) {
      throw new IllegalArgumentException("Tiered merging needs a fanout of at least 2");
    }
    this.kind = kind;
    this.sealBytes = sealBytes;
    this.fanout = fanout;
  }

  public Kind kind() {
    return kind;
  }

  /** Plans a batch whose inserts are about {@code insertBytes}; does not change the stack. */
  public Plan plan(long insertBytes) {
    List<Unit> merge = new ArrayList<>();
    int level = 0;
    switch (kind) {
      case A -> {}
      case B -> {
        if (!stack.isEmpty()) {
          merge.add(stack.get(stack.size() - 1));
        }
      }
      case C -> {
        long bytes = insertBytes;
        int end = stack.size();
        while (true) {
          int need = fanout - 1;
          if (end < need) {
            break;
          }
          List<Unit> run = stack.subList(end - need, end);
          int lvl = level;
          long runBytes = run.stream().mapToLong(Unit::bytes).sum();
          if (!run.stream().allMatch(u -> u.level() == lvl) || bytes + runBytes > sealBytes) {
            break;
          }
          merge.addAll(0, run);
          bytes += runBytes;
          end -= need;
          level += 1;
        }
      }
    }
    return new Plan(merge, level);
  }

  /**
   * Records the batch's outcome: the merged units left; the new unit joins unless sealed. A sealed
   * new unit seals the older open units too: they sit before it, so merging them later would make
   * a unit spanning it, and the tail would no longer be the newest rows (in RMoW, row groups' id
   * ranges would overlap).
   */
  public void commit(Plan plan, long newId, long newBytes) {
    stack.removeAll(plan.merge());
    if (sealed(newBytes)) {
      stack.clear();
    } else {
      stack.add(new Unit(newId, plan.level(), newBytes));
    }
  }

  /**
   * Whether a unit of this size is done merging: under B once it reaches the seal size; under C
   * once merging {@code fanout} units of its size would pass it (it could never merge again, and
   * an open unit that never merges would only grow the stack). Under A nothing is ever open.
   */
  private boolean sealed(long bytes) {
    return switch (kind) {
      case A -> true;
      case B -> bytes >= sealBytes;
      case C -> bytes * fanout > sealBytes;
    };
  }

  /** A unit was rewritten (an update or delete touched it): it keeps its place and level. */
  public void replace(long oldId, long newId, long newBytes) {
    for (int i = 0; i < stack.size(); i++) {
      if (stack.get(i).id() == oldId) {
        if (newBytes == 0) {
          stack.remove(i);
        } else {
          stack.set(i, new Unit(newId, stack.get(i).level(), newBytes));
        }
        return;
      }
    }
  }

  public boolean isOpen(long id) {
    return stack.stream().anyMatch(u -> u.id() == id);
  }

  /** The tail rolled over to a new file: every open unit is sealed. */
  public void sealAll() {
    stack.clear();
  }

  public List<Unit> open() {
    return List.copyOf(stack);
  }
}
