import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bench_hybrid_retrieval_matrix.sh"
PREPARE = ROOT / "scripts" / "prepare_hybrid_dataset.py"


class BenchHybridRetrievalMatrixTests(unittest.TestCase):
    def _prepare_fixture(self, root: Path) -> Path:
        source = root / "source"
        (source / "qrels").mkdir(parents=True)
        (source / "corpus.jsonl").write_text(
            json.dumps({"_id": "d1", "title": "Apple", "text": "orchard fruit"})
            + "\n",
            encoding="utf-8",
        )
        (source / "queries.jsonl").write_text(
            json.dumps({"_id": "q1", "text": "apple"}) + "\n",
            encoding="utf-8",
        )
        (source / "qrels" / "test.tsv").write_text(
            "query-id\tcorpus-id\tscore\nq1\td1\t2\n",
            encoding="utf-8",
        )
        prepared = root / "datasets" / "scifact"
        subprocess.run(
            [
                sys.executable,
                str(PREPARE),
                "--source",
                str(source),
                "--output",
                str(prepared),
                "--dataset",
                "scifact",
                "--dense-backend",
                "hash",
                "--dense-dimensions",
                "4",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return prepared.parent

    def test_paid_cells_delete_cache_and_scratch_after_artifact_validation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            datasets = self._prepare_fixture(root)
            output = root / "results"
            tools = root / "tools"
            tools.mkdir()
            cargo = tools / "cargo"
            cargo.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            cargo.chmod(0o755)
            bench = tools / "hybrid-retrieval-fixture"
            bench.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "build" ]]; then
  mkdir -p "$BORSUK_HYBRID_OUTPUT"
  printf 'status\\nmeasured\\n' > "$BORSUK_HYBRID_OUTPUT/hybrid_build.csv"
  exit 0
fi
if find "$BORSUK_TEST_MATRIX_OUT" -type f \\( -path '*/cache/*' -o -path '*/scratch/*' \\) -print -quit | grep -q .; then
  echo "previous hybrid cell cache or scratch survived" >&2
  exit 86
fi
printf 'cache\\n' > "$BORSUK_HYBRID_CACHE_DIR/object"
printf 'scratch\\n' > "$TMPDIR/work"
for artifact in hybrid_queries.csv hybrid_summary.csv hybrid_startup.csv; do
  printf 'status\\nmeasured\\n' > "$BORSUK_HYBRID_OUTPUT/$artifact"
done
""",
                encoding="utf-8",
            )
            bench.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{tools}{os.pathsep}{env['PATH']}",
                    "BORSUK_HYBRID_BINARY": str(bench),
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "1",
                    "BORSUK_RUN_HYBRID_MATRIX": "1",
                    "BORSUK_S3_BUCKET": "s3://fixture",
                    "BORSUK_HYBRID_DATASETS_ROOT": str(datasets),
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "BORSUK_HYBRID_MATRIX_PROFILES": "srht",
                    "BORSUK_HYBRID_MATRIX_MODES": "dense",
                    "BORSUK_HYBRID_SEARCH_POINTS": "128:32",
                    "BORSUK_HYBRID_HOT_FRACTIONS": "0 1",
                    "BORSUK_HYBRID_REPETITIONS": "1",
                    "BORSUK_TEST_MATRIX_OUT": str(output),
                    "OUT": str(output),
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            query_roots = sorted(output.glob("scifact/srht/query/**/repetition-1"))
            self.assertEqual(len(query_roots), 2)
            for query_root in query_roots:
                for artifact in (
                    "hybrid_queries.csv",
                    "hybrid_summary.csv",
                    "hybrid_startup.csv",
                    "resources.csv",
                ):
                    self.assertTrue((query_root / artifact).is_file())
                self.assertFalse((query_root / "cache").exists())
                self.assertFalse((query_root / "scratch").exists())

    def test_cell_cleanup_follows_all_durable_artifact_checks(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        cleanup = 'rm -rf "$cache_dir" "$query_out/scratch"'
        self.assertIn(cleanup, source)
        cell = source[source.index('test -s "$query_out/hybrid_queries.csv"') :]
        for artifact_check in (
            'test -s "$query_out/hybrid_queries.csv"',
            'test -s "$query_out/hybrid_summary.csv"',
            'test -s "$query_out/hybrid_startup.csv"',
            'test -s "$resource_path"',
        ):
            self.assertLess(cell.index(artifact_check), cell.index(cleanup))

    def test_dry_run_enumerates_mode_search_and_cache_fraction_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            datasets = root / "datasets"
            (datasets / "scifact").mkdir(parents=True)
            output = root / "results"
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "0",
                    "BORSUK_HYBRID_DATASETS_ROOT": str(datasets),
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "BORSUK_HYBRID_MATRIX_PROFILES": "srht",
                    "BORSUK_HYBRID_MATRIX_MODES": "dense sparse+text",
                    "BORSUK_HYBRID_SEARCH_POINTS": "128:32",
                    "BORSUK_HYBRID_HOT_FRACTIONS": "0 0.5",
                    "BORSUK_HYBRID_RRF_KS": "1 60",
                    "BORSUK_HYBRID_REPETITIONS": "1",
                    "OUT": str(output),
                }
            )
            subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            with (output / "coverage.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 7)
            self.assertEqual(rows[0]["stage"], "build")
            query_rows = rows[1:]
            self.assertEqual(
                {row["mode"] for row in query_rows}, {"dense", "sparse+text"}
            )
            self.assertEqual(
                {row["target_hot_query_fraction"] for row in query_rows},
                {"0", "0.5"},
            )
            self.assertEqual({row["rrf_k"] for row in query_rows}, {"1", "60"})
            self.assertEqual(
                {row["rrf_k"] for row in query_rows if row["mode"] == "dense"},
                {"1"},
            )
            self.assertTrue(all(row["candidate_depth"] == "128" for row in query_rows))
            self.assertTrue(all(row["max_segments"] == "32" for row in query_rows))
            self.assertTrue(all(row["status"] == "planned" for row in rows))

    def test_dry_run_uses_independent_seeded_repetition_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            datasets = root / "datasets"
            (datasets / "scifact").mkdir(parents=True)
            output = root / "results"
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "0",
                    "BORSUK_HYBRID_DATASETS_ROOT": str(datasets),
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "BORSUK_HYBRID_MATRIX_PROFILES": "srht",
                    "BORSUK_HYBRID_MATRIX_MODES": "dense",
                    "BORSUK_HYBRID_SEARCH_POINTS": "128:32",
                    "BORSUK_HYBRID_HOT_FRACTIONS": "0",
                    "BORSUK_HYBRID_REPETITIONS": "2",
                    "BORSUK_HYBRID_MASTER_SEED": "100",
                    "OUT": str(output),
                }
            )
            subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, check=True)
            with (output / "coverage.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            query_rows = [row for row in rows if row["stage"] == "query"]
            self.assertEqual(
                [row["campaign_repetition"] for row in query_rows], ["1", "2"]
            )
            self.assertEqual([row["query_seed"] for row in query_rows], ["101", "102"])
            self.assertEqual(len({row["artifact_dir"] for row in query_rows}), 2)
            self.assertTrue(query_rows[0]["artifact_dir"].endswith("repetition-1"))
            self.assertTrue(query_rows[1]["artifact_dir"].endswith("repetition-2"))

    def test_paid_mode_requires_explicit_guard_and_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "1",
                    "BORSUK_HYBRID_DATASETS_ROOT": temporary,
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "OUT": str(Path(temporary) / "out"),
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("paid execution", completed.stderr.lower())


if __name__ == "__main__":
    unittest.main()
