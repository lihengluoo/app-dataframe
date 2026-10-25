#!/usr/bin/env python3
"""Check every aggregate against Python, without assuming preview row order."""

import collections
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


column_names = ("id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3")
query_specs = (
    (("id1",), (("sum", "v1"),)),
    (("id1", "id2"), (("sum", "v1"),)),
    (("id3",), (("sum", "v1"), ("sum", "v3"))),
    (("id4",), (("sum", "v1"), ("sum", "v3"))),
    (("id6",), (("sum", "v1"), ("sum", "v3"))),
    (("id4", "id5"), (("sum", "v3"), ("min", "v3"))),
    (("id3",), (("min", "v1"), ("min", "v2"))),
    (("id6",), (("min", "v3"),)),
    (("id2", "id4"), (("sum", "v1"), ("sum", "v2"))),
    (("id1", "id2", "id3", "id4", "id5", "id6"),
     (("sum", "v1"), ("sum", "v3"))),
    (("id1",), (("mean", "v1"), ("mean", "v3"),
                ("count", "v3"), ("max", "v2"))),
    (("id4", "id5"), (("sum", "v1"), ("min", "v2"),
                       ("max", "v3"), ("count_all", "v1"))),
    (("id3",), (("mean", "v3"), ("count", "v3"))),
)


def aggregate_cells(rows, operation, column):
    cells = [row[column] for row in rows if row[column] is not None]
    if operation == "sum":
        return math.fsum(cells)
    if operation == "min":
        return min(cells, default=None)
    if operation == "max":
        return max(cells, default=None)
    if operation == "count":
        return len(cells)
    if operation == "count_all":
        return len(rows)
    if operation == "mean":
        return math.fsum(cells) / len(cells) if cells else None
    raise AssertionError(operation)


def compute_groups(rows, keys, aggregates):
    grouped_rows = collections.defaultdict(list)
    key_columns = [column_names.index(name) for name in keys]
    for row in rows:
        grouped_rows[tuple(row[column] for column in key_columns)].append(row)
    return {
        key: tuple(aggregate_cells(members, operation, column_names.index(name))
                   for operation, name in aggregates)
        for key, members in grouped_rows.items()
    }


class dataframe_tests(unittest.TestCase):
    binary = Path(sys.argv[1]).resolve()

    def setUp(self):
        temp_root = Path("/tmp/opencode")
        directory = tempfile.TemporaryDirectory(
            prefix="dataframe-test-", dir=temp_root if temp_root.is_dir() else None,
        )
        self.addCleanup(directory.cleanup)
        self.csv_path = Path(directory.name) / "input.csv"

    def write_csv(self, rows):
        self.csv_path.write_text(
            ",".join(column_names) + "\n" + "".join(
                ",".join("NULL" if cell is None else str(cell) for cell in row) + "\n"
                for row in rows
            )
        )

    def run_dataframe(self, thread_count, partition_count):
        return subprocess.run(
            [str(self.binary), "--threads", str(thread_count),
             "--partitions", str(partition_count), "--suite", "extended",
             "--passes", "2", str(self.csv_path)],
            capture_output=True, text=True, timeout=10,
        )

    def check_output(self, result, rows, thread_count, partition_count):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stderr, "")
        lines = iter(result.stdout.splitlines())
        query_count = len(query_specs) * 2
        self.assertEqual(
            next(lines, ""),
            f"engine=polars_c rows={len(rows)} threads={thread_count} "
            f"partitions={partition_count} queries={query_count}",
        )
        expected_queries = [compute_groups(rows, keys, aggregates)
                            for keys, aggregates in query_specs]
        for query_index in range(query_count):
            op = query_index % len(query_specs)
            keys, aggregates = query_specs[op]
            expected = expected_queries[op]
            with self.subTest(query=query_index + 1, op=op + 1):
                self.assertEqual(
                    next(lines, ""),
                    f"query={query_index + 1} op=Q{op + 1} groups={len(expected)}",
                )
                header = list(keys) + [
                    "count(*)" if operation == "count_all" else f"{operation}({name})"
                    for operation, name in aggregates
                ]
                self.assertEqual(next(lines, ""), ",".join(header))
                printed_keys = set()
                for _ in range(min(5, len(expected))):
                    line = next(lines, "")
                    cells = line.split(",")
                    self.assertEqual(len(cells), len(header), line)
                    key = tuple(None if cell == "NULL" else int(cell)
                                for cell in cells[:len(keys)])
                    self.assertIn(key, expected, line)
                    self.assertNotIn(key, printed_keys, line)
                    printed_keys.add(key)
                    for cell, number, (operation, _) in zip(
                        cells[len(keys):], expected[key], aggregates,
                    ):
                        if number is None:
                            self.assertEqual(cell, "NULL", line)
                        elif operation in ("count", "count_all"):
                            self.assertEqual(int(cell), number, line)
                        else:
                            self.assertTrue(math.isclose(float(cell), number,
                                                        rel_tol=1e-12, abs_tol=1e-12),
                                            line)
                if len(expected) <= 5:
                    self.assertEqual(printed_keys, set(expected))
        summary = dict(field.split("=", 1) for field in next(lines, "").split())
        self.assertEqual(set(summary), {
            "total_queries", "processed_rows", "query_ms",
            "query_throughput_rows_per_sec", "application_ms",
            "application_throughput_rows_per_sec",
        })
        self.assertEqual(int(summary["total_queries"]), query_count)
        self.assertEqual(int(summary["processed_rows"]), len(rows) * query_count)
        for name in ("query_ms", "query_throughput_rows_per_sec", "application_ms",
                     "application_throughput_rows_per_sec"):
            number = float(summary[name])
            self.assertTrue(math.isfinite(number), summary)
            self.assertGreaterEqual(number, 0, summary)
        self.assertEqual(list(lines), [])

    def test_aggregate_queries(self):
        keys = (
            (None, None, None, None, None, None),
            (0, 0, 0, 0, 0, 0),
            (1, 10, 100, 1000, 10000, 100000),
            (1, 11, 100, 1001, 10001, 100001),
            (0, None, 0, 0, 0, None),
        )
        batches = (
            ((None, None, None), (0, None, 0.0), (-4, 7, 1.25),
             (5, -9, None), (None, 2, None)),
            ((None, None, None), (3, -3, None), (None, None, -2.5),
             (-6, 4, 6.5), (8, None, None)),
            ((None, None, None), (None, 0, -1.5), (10, -1, 0.0),
             (None, None, 2.25), (-2, 5, None)),
        )
        # Repeated key tuples are interleaved across the original input shards.
        # At most five groups makes the preview the complete result for every Q.
        rows = [key + measures for batch in batches for key, measures in zip(keys, batch)]
        self.write_csv(rows)
        for thread_count, partition_count in ((1, 1), (4, 2), (3, 7)):
            with self.subTest(threads=thread_count, partitions=partition_count):
                result = self.run_dataframe(thread_count, partition_count)
                self.check_output(result, rows, thread_count, partition_count)

    def test_group_growth(self):
        # Enough distinct tuples to resize hash/group arrays; repeats also grow
        # each group's row-ID list. The five displayed groups may be any subset.
        rows = [
            (index, index % 13, index % 67, index % 89, index % 7, index % 127,
             None if (index + repeat) % 11 == 0 else (index * 3 + repeat) % 31 - 15,
             None if (index + repeat) % 5 == 0 else (index - repeat) % 23 - 11,
             None if (index + repeat) % 7 == 0 else ((index - repeat) % 37 - 18) / 8)
            for repeat in range(6) for index in range(1025)
        ]
        self.write_csv(rows)
        result = self.run_dataframe(3, 5)
        self.check_output(result, rows, 3, 5)

    def test_numeric_threading_threshold(self):
        for count in (1000, 1001):
            with self.subTest(rows=count):
                rows = [
                    (None if index % 5 == 0 else index % 5,) * 6 +
                    (index % 7 - 3, index % 11 - 5, (index % 13 - 6) / 8)
                    for index in range(count)
                ]
                self.write_csv(rows)
                result = self.run_dataframe(3, 7)
                self.check_output(result, rows, 3, 7)

    def test_empty_shards(self):
        rows = [(4294967295, 0, None, 65536, 4294967295, 1,
                 -2147483648, 2147483647, -1.25)]
        self.write_csv(rows)
        result = self.run_dataframe(4, 7)
        self.check_output(result, rows, 4, 7)

    def test_header_only(self):
        self.write_csv([])
        result = self.run_dataframe(4, 7)
        self.check_output(result, [], 4, 7)

    def test_invalid_input(self):
        cases = (
            ("empty", "", r"^Cannot read a nine-column H2O CSV header and input\.\n$"),
            ("invalid_number", ",".join(column_names) + "\n0,0,0,0,0,0,bad,1,2\n",
             r"^dataframe failed: error=[13] \(1=input, 2=allocation, 3=peer\)\.\n$"),
            ("missing_column", ",".join(column_names) + "\n0,0,0,0,0,0,1,2\n",
             r"^dataframe failed: error=[13] \(1=input, 2=allocation, 3=peer\)\.\n$"),
        )
        # Malformed one-row inputs leave other workers' shards empty, but all
        # workers must still complete the repeated-query barrier schedule.
        for name, text, message in cases:
            with self.subTest(input=name):
                self.csv_path.write_text(text)
                result = self.run_dataframe(4, 2)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual(result.stderr, "")
                self.assertRegex(result.stdout, message)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
