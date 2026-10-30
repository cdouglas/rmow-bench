package org.rmow.bench.arms;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.rmow.bench.io.Held;

/**
 * The position deletes one read (or one writer's scan) has loaded, by data file and delete files,
 * shared by its scans ({@link Reader#deleteCache}); what it loaded is held until it closes.
 */
final class DeleteCache implements AutoCloseable {

  final Map<String, PositionDeleteIndex> loaded = new ConcurrentHashMap<>();
  final Held.Scope held = Held.scope("deletes released");

  @Override
  public void close() {
    held.close();
  }
}
