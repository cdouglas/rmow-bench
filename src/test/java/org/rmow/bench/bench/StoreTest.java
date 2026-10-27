package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Which stores append, decided before any request. */
class StoreTest {

  @Test
  void onlyDirectoryBucketsAppend() {
    assertThat(Store.directoryBucket("s3://rmow-bench--use1-az4--x-s3/prefix")).isTrue();
    assertThat(Store.directoryBucket("s3://rmow-bench--use1-az4--x-s3")).isTrue();
    assertThat(Store.directoryBucket("s3://bench/runs")).isFalse();
  }

  /** Standard S3 refuses RMoW's appendable arms before sending anything. */
  @Test
  void rmowNeedsAppend(@TempDir Path dir) {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "s3";
    cfg.store.root = "s3://bench/runs";
    cfg.store.region = "us-east-1";
    cfg.arms = List.of("mor-v3", "rmow-c");
    assertThatThrownBy(() -> new Runner(cfg, dir, "t").run())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("S3 Express");
  }
}
