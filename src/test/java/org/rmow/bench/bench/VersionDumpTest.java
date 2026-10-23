package org.rmow.bench.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Every footer the RMoW arms write, under both footer orders: the runner dumps each version and
 * checks the versions each step held against the reference model, then pyarrow and DuckDB read
 * every version ({@code analysis/interop_check.py}; skipped without the {@code .venv}). Split
 * files' footer-only versions are reassembled by pyarrow's dataset API and refused by its plain
 * reader and by DuckDB.
 */
class VersionDumpTest {

  @ParameterizedTest
  @ValueSource(strings = {"logical", "offset"})
  void everyVersionReadsBackInOtherReaders(String order, @TempDir Path dir)
      throws IOException, InterruptedException {
    BenchConfig cfg = new BenchConfig();
    cfg.store.type = "mem";
    cfg.store.root = "mem://dump";
    cfg.workload.initialRows = 20_000;
    cfg.workload.batches = 10;
    cfg.workload.opsPerBatch = 1_000;
    cfg.layout.rowGroupBytes = 128 << 10;
    cfg.layout.targetFileBytes = 1 << 20;
    cfg.layout.tailSealBytes = 64 << 10;
    cfg.layout.tierFanout = 3;
    cfg.layout.compactEvery = 4;
    cfg.layout.compactSmallRowGroups = 4;
    cfg.layout.footerOrder = order;
    cfg.arms =
        List.of("rmow-a", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split", "mor-v3");
    cfg.reads.every = 5;
    new Runner(cfg, dir, "t", new Runner.Options(null, false, true)).run();

    Path versions = dir.resolve("versions");
    List<String> rows = Files.readAllLines(versions.resolve("versions.csv"));
    assertThat(rows.get(0)).startsWith("arm,object,version,length");
    // every RMoW arm, no other: each adopts the initial files and writes at least one version
    assertThat(rows.subList(1, rows.size()))
        .extracting(r -> r.split(",")[0])
        .containsOnly("rmow-a", "rmow-b", "rmow-c", "rmow-b-split", "rmow-c-split");
    // updates rewrite sealed row groups in place only under logical order
    boolean anyUnordered = rows.stream().skip(1).anyMatch(r -> r.split(",")[10].equals("false"));
    assertThat(anyUnordered).isEqualTo(order.equals("logical"));

    Path python = Path.of(".venv/bin/python");
    assumeTrue(Files.isExecutable(python), "no .venv: pyarrow and DuckDB not checked");
    Process p =
        new ProcessBuilder(
                python.toString(), "analysis/interop_check.py", versions.toString(), "-j", "4")
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(p.waitFor(5, TimeUnit.MINUTES)).isTrue();
    assertThat(p.exitValue()).as(out).isZero();
    assertThat(out).contains(" 0 failed");
  }
}
