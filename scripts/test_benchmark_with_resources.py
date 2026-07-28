import os
import tempfile
import unittest
from pathlib import Path

from benchmark_with_resources import (
    directory_bytes,
    parse_proc_io,
    parse_proc_net_dev,
    parse_proc_stat,
    parse_ps_process_tree,
    parse_ps_time,
    sample_process_tree,
)


class ResourceSamplerTest(unittest.TestCase):
    def test_parses_portable_ps_cpu_time(self) -> None:
        self.assertAlmostEqual(parse_ps_time("01:02.50"), 62.5)
        self.assertAlmostEqual(parse_ps_time("2-03:04:05.25"), 183845.25)

    def test_parses_portable_ps_tree_for_macos_fallback(self) -> None:
        output = """100 1 00:01.00 10 20
101 100 00:02.50 30 40
102 101 00:00.25 50 60
999 1 10:00.00 999 999
"""
        ticks, rss, vms, read_bytes, write_bytes = parse_ps_process_tree(
            output, 100, clock_ticks=100
        )
        self.assertEqual(ticks, 375)
        self.assertEqual(rss, (10 + 30 + 50) * 1024)
        self.assertEqual(vms, (20 + 40 + 60) * 1024)
        self.assertEqual((read_bytes, write_bytes), (0, 0))

    def test_parses_proc_stat_after_parenthesized_command(self) -> None:
        fields = ["R"] + [str(value) for value in range(4, 53)]
        fields[11] = "100"
        fields[12] = "25"
        fields[20] = "4096"
        fields[21] = "7"
        self.assertEqual(
            parse_proc_stat("42 (bench worker) " + " ".join(fields)), (125, 4096, 7)
        )

    def test_parses_physical_io_bytes(self) -> None:
        self.assertEqual(
            parse_proc_io("rchar: 99\nread_bytes: 12\nwrite_bytes: 34\n"), (12, 34)
        )

    def test_parses_aggregate_network_bytes_without_loopback(self) -> None:
        value = """Inter-| Receive | Transmit
 face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed
    lo: 100 1 0 0 0 0 0 0 200 1 0 0 0 0 0 0
  eth0: 1234 2 0 0 0 0 0 0 5678 3 0 0 0 0 0 0
  ens5: 10 1 0 0 0 0 0 0 20 1 0 0 0 0 0 0
"""
        self.assertEqual(parse_proc_net_dev(value), (1244, 5698))

    def test_directory_bytes_sums_nested_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nested").mkdir()
            (root / "one").write_bytes(b"123")
            (root / "nested" / "two").write_bytes(b"4567")
            self.assertEqual(directory_bytes(root), 7)

    def test_samples_the_launcher_and_all_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)

            def write_process(
                pid: int,
                parent: int,
                ticks: int,
                rss_pages: int,
                vms_bytes: int,
                read_bytes: int,
                write_bytes: int,
            ) -> None:
                process = proc / str(pid)
                process.mkdir()
                fields = ["S"] + ["0"] * 48
                fields[1] = str(parent)
                fields[11] = str(ticks)
                fields[12] = "0"
                fields[20] = str(vms_bytes)
                fields[21] = str(rss_pages)
                (process / "stat").write_text(
                    f"{pid} (worker {pid}) " + " ".join(fields)
                )
                (process / "io").write_text(
                    f"read_bytes: {read_bytes}\nwrite_bytes: {write_bytes}\n"
                )

            write_process(100, 1, 10, 2, 1_000, 100, 200)
            write_process(101, 100, 20, 3, 2_000, 300, 400)
            write_process(102, 101, 30, 5, 4_000, 500, 600)
            write_process(999, 1, 1_000, 100, 100_000, 10_000, 20_000)

            self.assertEqual(
                sample_process_tree(100, proc),
                (60, 10 * os.sysconf("SC_PAGE_SIZE"), 7_000, 900, 1_200),
            )


if __name__ == "__main__":
    unittest.main()
