import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/bench_publication_v2_aws.sh"


class BenchPublicationV2AwsTests(unittest.TestCase):
    def _manifest(self, root: Path) -> Path:
        value = json.loads(
            (ROOT / "docs/research/publication-v2-manifest.json").read_text(
                encoding="utf-8"
            )
        )
        value.update(
            campaign_id="publication-v2-test-campaign",
            result_prefix="publication/v2/test-campaign/results",
            index_prefix="publication/v2/test-campaign/indexes",
            cache_prefix="publication-v2-test-campaign-cache",
        )
        manifest = root / "manifest.json"
        manifest.write_text(json.dumps(value), encoding="utf-8")
        return manifest

    def test_dry_run_freezes_schedule_before_any_paid_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "results"
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env={
                    **os.environ,
                    "BORSUK_PUBLICATION_V2_EXECUTE": "0",
                    "BORSUK_PUBLICATION_V2_ROOT": str(output),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue((output / "manifest.json").is_file())
            self.assertTrue((output / "schedule.csv").is_file())
            self.assertTrue((output / "environment.txt").is_file())

    def test_dry_run_rejects_campaign_and_prefix_drift(self):
        for variable, value, message in [
            (
                "BORSUK_PUBLICATION_V2_RUN_ID",
                "different-campaign",
                "campaign id mismatch",
            ),
            (
                "BORSUK_PUBLICATION_V2_RESULT_PREFIX",
                "publication/v2/wrong/results",
                "result prefix mismatch",
            ),
            (
                "BORSUK_PUBLICATION_V2_INDEX_PREFIX",
                "publication/v2/wrong/indexes",
                "index prefix mismatch",
            ),
        ]:
            with (
                self.subTest(variable=variable),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                completed = subprocess.run(
                    ["bash", str(SCRIPT)],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "BORSUK_PUBLICATION_V2_MANIFEST": str(self._manifest(root)),
                        "BORSUK_PUBLICATION_V2_EXECUTE": "0",
                        "BORSUK_PUBLICATION_V2_ROOT": str(root / "results"),
                        variable: value,
                    },
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(message, completed.stderr.lower())

    def test_paid_mode_is_explicitly_gated(self):
        completed = subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=ROOT,
            env={**os.environ, "BORSUK_PUBLICATION_V2_EXECUTE": "1"},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("BORSUK_RUN_PUBLICATION_V2=1", completed.stderr)

    def test_source_contract_is_confirmatory_and_has_no_external_controls(self):
        source = SCRIPT.read_text(encoding="utf-8")
        hybrid_requirements = (
            ROOT / "scripts" / "requirements-hybrid-bench.txt"
        ).read_text(encoding="utf-8")
        self.assertLess(
            source.index("publication_protocol.py validate"), source.index("aws ")
        )
        self.assertIn("BORSUK_BENCH_QUERIES=1000", source)
        self.assertIn("BORSUK_BENCH_UNCACHED_QUERIES=1000", source)
        self.assertGreaterEqual(source.count("BORSUK_FULL_RECALL_ONLY=1"), 2)
        self.assertGreaterEqual(source.count("BORSUK_FULL_SKIP_EXACT_RECALL=1"), 2)
        self.assertIn("BORSUK_S3V_DATASETS=fashion-mnist-784", source)
        self.assertIn("run_borsuk_direct", source)
        self.assertIn("run_borsuk_remaining", source)
        self.assertIn('BORSUK_FULL_DATASET_NAMES="$dense_order"', source)
        self.assertIn(
            'BORSUK_HYBRID_MATRIX_DATASETS="$hybrid_dataset_order"',
            source,
        )
        self.assertIn("BORSUK_HYBRID_SEARCH_POINTS=256:64", source)
        self.assertIn('BORSUK_HYBRID_HOT_FRACTIONS="0 0.5 1"', source)
        self.assertIn("BORSUK_HYBRID_RRF_KS=60", source)
        self.assertIn("dense_dataset_order", source)
        self.assertIn("hybrid_dataset_order", source)
        self.assertIn("ram_bytes=", source)
        self.assertIn("instance_type=${BORSUK_INSTANCE_TYPE", source)
        self.assertIn("accelerator=${BORSUK_ACCELERATOR", source)
        self.assertIn("index_storage_class=${BORSUK_INDEX_STORAGE_CLASS", source)
        self.assertIn("managed_service_compute=undisclosed", source)
        self.assertIn("requirements-hybrid-bench.txt", source)
        self.assertIn('HYBRID_PYTHON_VERSION="${', source)
        self.assertIn("sha256_file()", source)
        self.assertIn("sha256sum", source)
        self.assertIn("uv python install", source)
        self.assertIn("uv venv --python", source)
        self.assertIn("uv pip install --python", source)
        self.assertIn("uv pip freeze --python", source)
        self.assertIn("hybrid Python version mismatch", source)
        self.assertIn(
            "boto3==1.42.97",
            hybrid_requirements,
            "the isolated publication environment must pin an S3 Vectors-capable SDK",
        )
        self.assertIn("fetch_beir_dataset.py", source)
        self.assertIn("list_vector_buckets", source)
        self.assertLess(
            source.index('for prefix in "$RESULT_PREFIX" "$INDEX_PREFIX"'),
            source.index('uv pip install --python "$HYBRID_VENV/bin/python3"'),
            "immutable result prefixes must be checked before environment setup",
        )
        self.assertLess(
            source.index("trap publication_exit EXIT"),
            source.index('uv pip install --python "$HYBRID_VENV/bin/python3"'),
            "failed preflight and dataset preparation must retain evidence",
        )
        self.assertLess(
            source.index("list_vector_buckets"),
            source.index("fetch_beir_dataset.py"),
            "S3 Vectors availability must fail before dataset preparation",
        )
        self.assertIn("prepare_hybrid_dataset.py", source)
        self.assertIn("validate_hybrid_dataset.py", source)
        self.assertIn("5c38ec7c405ec4b44b94cc5a9bb96e735b38267a", source)
        self.assertIn("hybrid-inputs", source)
        self.assertIn("--expected-repetitions 5", source)
        self.assertIn("--expected-queries 1000", source)
        self.assertIn("query_samples.csv", source)
        self.assertIn("source-archive", source)
        self.assertIn("schedule.csv", source)
        self.assertIn("manifest.json", source)
        self.assertIn('[[ "$result_key" == "$RESULT_PREFIX/$repetition_id" ]]', source)
        self.assertIn('[[ "$index_key" == "$INDEX_PREFIX/$repetition_id" ]]', source)
        self.assertIn('[[ "$cache_key" == "$CACHE_PREFIX-$repetition_id" ]]', source)
        self.assertIn("trap publication_exit EXIT", source)
        self.assertIn("trap 'exit 130' INT TERM HUP", source)
        self.assertNotIn("bench_external_control_matrix", source)
        self.assertNotIn("/pilot/", source)

    def test_direct_pair_is_adjacent_and_sync_precedes_cleanup(self):
        source = SCRIPT.read_text(encoding="utf-8")
        loop = source[source.index('if [[ "$system_order"') :]
        direct_pair = loop[: loop.index("run_borsuk_remaining")]
        self.assertIn("run_borsuk_direct", direct_pair)
        self.assertIn("run_s3_vectors", direct_pair)
        self.assertNotIn("run_hybrid", direct_pair)
        self.assertLess(
            loop.index("sync_results"),
            loop.index("cleanup_repetition_data"),
        )


if __name__ == "__main__":
    unittest.main()
