# Dataframe group-by benchmark

C11 numeric group-by and aggregation benchmark adapted from the Polars
rs-0.51.0 kernels. Persistent pthread workers load separate CSV shards,
exchange grouping keys and original row IDs through hash partitions, and
build complete group indices before aggregating the original columns.
Barriers separate input loading, exchange writes, and exchange reads.

Each worker keeps its full columnar query results until the last query
finishes. Main prints group counts and up to five unsorted rows per query;
it receives copied previews after all workers join. Increasing the pass
count also increases the memory needed for saved results.

## Build and run

Requires a C11 compiler, POSIX pthread barriers and `pread`, and libm.
Commands below run from the Delphi repository root:

```sh
make -C applications/dataframe
applications/dataframe/build/dataframe --help
applications/dataframe/build/dataframe --threads 4 --suite extended input.csv
```

The Makefile also builds directly from this submodule with `make`.
`build_dir`, `CC`, `CPPFLAGS`, `CFLAGS`, `LDFLAGS`, and `LDLIBS` can be
overridden. `make clean` removes the selected build directory and wllvm
object intermediates.

## CSV input

The input is a regular file that stays unchanged during the run. A header
and nine comma-separated columns in this order are required:

```csv
id1,id2,id3,id4,id5,id6,v1,v2,v3
id001,id001,id001,1,1,1,10,20,1.5
id001,id002,id001,1,1,2,30,40,2.5
id002,id001,id002,2,2,1,-5,10,3.5
```

The six keys are unsigned 32-bit integers, optionally prefixed with `id`.
`v1` and `v2` are signed 32-bit integers; `v3` is a finite double in decimal
notation. Empty fields, `NA`, `NULL`, `na`, and
`null` mean missing data. Missing keys form groups like other keys;
aggregates skip missing inputs, while `count(*)` counts every row.

LF, CRLF, blank lines, and a final row without a newline are supported.
Quoted fields, embedded newlines, and whitespace around numbers are not
supported. No dataset is bundled or downloaded.

## Workload options

| Option | Behavior |
| --- | --- |
| `--threads N` | Worker count, 1–4096; defaults to 8. |
| `--partitions P` | Positive hash partition count; defaults to the worker count. |
| `--passes R` | Repeat the selected schedule R times; defaults to 1. |
| `--suite drust` | Default: Q10 twice, then Q1–Q9 three times, for 29 queries. |
| `--suite extended` | Run Q1–Q13 once. |
| `--query Q` | Run one query, 1–13, per pass; overrides the suite. |

All workers cooperate on each query; queries run sequentially. The query
definitions live in `dataframe.h`:

| Query | Grouping keys | Aggregates |
| --- | --- | --- |
| Q1 | id1 | sum(v1) |
| Q2 | id1, id2 | sum(v1) |
| Q3 | id3 | sum(v1), sum(v3) |
| Q4 | id4 | sum(v1), sum(v3) |
| Q5 | id6 | sum(v1), sum(v3) |
| Q6 | id4, id5 | sum(v3), min(v3) |
| Q7 | id3 | min(v1), min(v2) |
| Q8 | id6 | min(v3) |
| Q9 | id2, id4 | sum(v1), sum(v2) |
| Q10 | id1–id6 | sum(v1), sum(v3) |
| Q11 | id1 | mean(v1), mean(v3), count(v3), max(v2) |
| Q12 | id4, id5 | sum(v1), min(v2), max(v3), count(*) |
| Q13 | id3 | mean(v3), count(v3) |

Reductions return doubles except counts.

The final line reports processed rows, elapsed milliseconds, and throughput.
`query_ms` spans the worker query loop and workspace/exchange cleanup.
`application_ms` also includes input loading, thread creation/join, preview
printing, and release of input columns and full results.

## LLVM IR capture

```sh
make -C applications/dataframe ir
```

The output is `build/ir/dataframe.ll`, captured by the repository-wide
`scripts/build.sh` driving this Makefile's `benchmark` target with wllvm.
Capture flags and normalization remain in that shared script. The default
toolchain is under `dependencies/install`; `llvm_root`, `wllvm`, and
`ir_dir` select other toolchain and output paths. IR capture requires this
submodule to be checked out under Delphi's `applications/` directory.
