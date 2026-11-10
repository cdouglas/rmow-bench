#!/usr/bin/env python3
"""Calibrates the fuzzer (bin/fuzz.sh): plants each known bug in a copy of the source, builds it,
and fuzzes it with a fixed budget of seeds. Each mutant must be caught (a seed fails) by the check
it names; `none` (no mutant) must stay clean. Run from the repository root, after `mvn package`
(the copies run on a copy of target/lib/, taken at the start):

    bin/fuzz-mutants.py [--seeds 60] [--workers 6] [--profile mixed|heavy] [--out DIR] [NAME ...]

A check named with a leading `?` is measured, not required: the mutant is subtler than random
draws can be expected to reach at this budget, and is reported either way. A mutant's check may
differ by profile (a dict); a mutant may run a multiple of the budget (its sixth field), when its
catches are random (a nondeterministic mutant) rather than fixed by the seeds.

The budgets each mutant needs, as found on 2026-11-08 (c2e1e66): at 60 seeds of the default draws
every required mutant is caught; RMoW packing open row groups is caught by about one seed in 250
to 500 (a round on havoc, not 60 seeds); a row group dead but for interior rows (ends-only) needs
the heavy profile, which catches it in about one seed in 12; one dead but for its second row has
not been caught in about 1,000 heavy seeds. The cut's two (2026-11-10, strides of 2 to 64 a row
group): 8 and 2 of the 60 seeds. The sorted read's skip and the locate by key (2026-11-10): see
calibration.txt of their first round.

Each mutant is an exact-text replacement anchored in the code as this file's last commit left it;
a replacement that no longer matches is reported, not skipped. Results: DIR/<mutant>/summary-*.jsonl
and DIR/calibration.txt.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ARMS = "src/main/java/org/rmow/bench/arms/"

# name: (file, old, new, the check expected to catch it (or a dict of them by profile), what the
#        bug is[, a multiple of the budget of seeds])
MUTANTS = {
    "none": (None, None, None, "-", "no mutant: must stay clean"),
    "rmor-drops-version-0": (
        ARMS + "TailCowArm.java",
        "      if (version >= 0) {\n        out.add(v == null ? r : new long[] {r[0], version});",
        "      if (version > 0) {\n        out.add(v == null ? r : new long[] {r[0], version});",
        "run",
        "a region rewrite drops the rows still at version 0",
    ),
    "rmor-dv-misses-last-row": (
        ARMS + "TailCowArm.java",
        "      for (long p = r.first(); p < r.first() + r.rows(); p++) {",
        "      for (long p = r.first(); p < r.first() + r.rows() - 1; p++) {",
        "run",
        "the DV over a rewritten row group misses its last row",
    ),
    "deferred-fold-dv-inverted": (
        ARMS + "TailCowArm.java",
        "          if (!versions.containsKey(row[0])) {\n            dvPositions",
        "          if (versions.containsKey(row[0])) {\n            dvPositions",
        "run",
        "a merge folding waiting rows puts DVs on the wrong ones",
    ),
    "rmow-packs-open-row-groups": (
        ARMS + "RmowArm.java",
        "      boolean packable = !policy.isOpen(rg) && size < target / 2;",
        "      boolean packable = size < target / 2;",
        "?run",
        "RMoW's packing takes row groups the tail policy still holds open (measured: about one"
        " seed in 250 to 500 of the default draws, on havoc)",
    ),
    "mor-compaction-keeps-old-file": (
        ARMS + "MorArm.java",
        "      for (int k : g) {\n        retire(k);\n      }",
        "      for (int k : g) {\n        if (g.size() > 1) {\n          retire(k);\n        }\n      }",
        "run",
        "a compaction rewriting one file for its deletes leaves the old file live",
    ),
    "reader-dead-off-by-one": (
        ARMS + "Scans.java",
        "d.recordCount() >= df.recordCount()",
        "d.recordCount() >= df.recordCount() - 1",
        "run",
        "a file with one live row counts as dead whole (skipping readers and writers)",
    ),
    "ranges-checks-ends-only": (
        ARMS + "DeletedRanges.java",
        "      long lo = key == first >>> 32 ? first & 0xFFFFFFFFL : 0;",
        "      long lo = key == first >>> 32 ? last & 0xFFFFFFFFL : 0;",
        {"mixed": "?run", "heavy": "run"},
        "a row group whose first and last rows are dead counts as dead whole",
    ),
    "ranges-skip-second-row": (
        ARMS + "DeletedRanges.java",
        "      long lo = key == first >>> 32 ? first & 0xFFFFFFFFL : 0;",
        "      long lo = key == first >>> 32 ? (first & 0xFFFFFFFFL) + 2 : 0;",
        "?run",
        "a row group dead but for its second row counts as dead whole (measured, not required:"
        " one interior row of a row group of at least a hundred)",
    ),
    "mor-compaction-nondeterministic": (
        ARMS + "MorArm.java",
        "groupBytes + liveSize > layout.targetFileBytes)",
        "groupBytes + liveSize > layout.targetFileBytes * (0.5 + Math.random()))",
        "twin",
        "compaction's bin-packing depends on a random draw: results right, counts not repeatable"
        " (about one seed in 60 shows it, at random: four times the budget)",
        4,
    ),
    "cut-drops-row-before-cut": (
        ARMS + "TailCowArm.java",
        "      int j = pieceOf[(int) ((p - r.first()) / stride)];\n      out.get(j).add(row);",
        "      int j = pieceOf[(int) ((p - r.first()) / stride)];\n"
        "      long next = (p - r.first() + 1) / stride;\n"
        "      if ((p - r.first() + 1) % stride == 0 && next < strides && pieceOf[(int) next] != j) {\n"
        "        continue;\n"
        "      }\n"
        "      out.get(j).add(row);",
        "run",
        "the cut drops the row just before each cut of a rewritten row group",
    ),
    "cut-drops-unspanned-folds": (
        ARMS + "TailCowArm.java",
        "      out.get(best).add(row);",
        "      if (bestGap == 0) {\n        out.get(best).add(row);\n      }",
        "run",
        "the cut drops a row folded in from a delta row group whose id no piece's rows span",
    ),
    "sorted-read-drops-live-row-groups": (
        ARMS + "Scans.java",
        "          if (dead.contains(FooterCodec.startOffset(rg))) {",
        "          if (!dead.isEmpty()) {",
        "run",
        "the sorted read under the skipping reader drops every row group of a file with one dead",
    ),
    "locate-keys-skips-first-key": (
        ARMS + "Scans.java",
        "      int first = i >= 0 ? i : -i - 1;",
        "      int first = i >= 0 ? i : -i;",
        "run",
        "the locate by key misses a row group whose only changed key is the first past its least id",
    ),
    "mor-stats-miss-deletes": (
        ARMS + "MorArm.java",
        "        files.size() + deleteObjects.size(),",
        "        files.size(),",
        "state|footprint",
        "the MoR family's footprint leaves out its delete files",
    ),
}


def build(name, out):
    src = out / name / "src-tree"
    if src.exists():
        shutil.rmtree(src)
    src.mkdir(parents=True)
    shutil.copy("pom.xml", src / "pom.xml")
    shutil.copytree("src/main", src / "src/main")
    path, old, new = MUTANTS[name][:3]
    if path:
        f = src / path
        text = f.read_text()
        if text.count(old) != 1:
            return f"anchor matches {text.count(old)} times in {path}"
        f.write_text(text.replace(old, new))
    r = subprocess.run(["mvn", "-q", "-o", "compile", "-f", str(src / "pom.xml")],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return "build failed:\n" + r.stdout[-2000:] + r.stderr[-2000:]
    return None


def expected(name, profile):
    """The check expected to catch the mutant under the profile ('-' for none)."""
    expect = MUTANTS[name][3]
    return expect.get(profile, "-") if isinstance(expect, dict) else expect


def budget(name, seeds):
    return seeds * (MUTANTS[name][5] if len(MUTANTS[name]) > 5 else 1)


def verdict(name, expect, failed):
    """The calibration's verdict on a mutant from its failed seeds' outcomes."""
    kinds = sorted({r["kind"] for r in failed})
    optional = expect.startswith("?")
    wanted = set(expect.lstrip("?").split("|"))
    if name == "none":
        return "clean" if not failed else "NOT CLEAN"
    if not failed:
        return "not caught (measured only)" if optional else "NOT CAUGHT"
    return "caught" if wanted & set(kinds) else f"caught, by {kinds} not {sorted(wanted)}"


def fuzz(name, out, seeds, workers, heap, profile):
    classes = out / name / "src-tree/target/classes"
    cp = f"{classes}:{(out / 'lib').resolve()}/*"
    procs = []
    for k in range(workers):
        cmd = ["java", f"-Xmx{heap}", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", cp,
               "org.rmow.bench.fuzz.Fuzz", "--out", str(out / name), "--from", "1",
               "--count", str(seeds), "--worker", str(k), "--workers", str(workers),
               "--profile", profile]
        procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for p in procs:
        p.wait()
    rows = []
    for f in (out / name).glob("summary-*.jsonl"):
        rows += [json.loads(line) for line in f.read_text().splitlines()]
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=60)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--heap", default="3g")
    ap.add_argument("--profile", default="mixed", help="the fuzzer's draws: mixed or heavy")
    ap.add_argument("--out", default="results/fuzz/calibration")
    ap.add_argument("names", nargs="*")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    # the dependencies, copied: a `mvn package` meanwhile rewrites target/lib under running JVMs
    if (out / "lib").exists():
        shutil.rmtree(out / "lib")
    shutil.copytree("target/lib", out / "lib")
    lines = []
    for name in a.names or MUTANTS:
        expect = expected(name, a.profile)
        err = build(name, out)
        if err:
            line = f"{name}: NOT BUILT: {err}"
        else:
            rows = fuzz(name, out, budget(name, a.seeds), a.workers, a.heap, a.profile)
            failed = sorted((r for r in rows if not r["ok"]), key=lambda r: r["seed"])
            kinds = sorted({r["kind"] for r in failed})
            first = failed[0] if failed else None
            line = (f"{name}: {verdict(name, expect, failed)}: {len(failed)} of {len(rows)} seeds failed"
                    + (f" ({', '.join(kinds)}); first seed {first['seed']}: "
                       f"{(first['message'] or '')[:160]}" if first else ""))
        print(line, flush=True)
        lines.append(line)
        (out / "calibration.txt").write_text("\n".join(lines) + "\n")
    ok = (": clean:", ": caught:", ": not caught (measured only):")
    return 0 if all(any(o in l for o in ok) for l in lines) else 1


if __name__ == "__main__":
    sys.exit(main())
