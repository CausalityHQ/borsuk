import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate_hybrid_synthetic_dataset.py"


class GenerateHybridSyntheticDatasetTests(unittest.TestCase):
    def run_generate(self, output: Path, scenario: str) -> None:
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--output",
                str(output),
                "--dataset",
                f"synthetic-{scenario}",
                "--scenario",
                scenario,
                "--documents",
                "60",
                "--queries",
                "12",
                "--topics",
                "6",
                "--dense-dimensions",
                "24",
                "--sparse-dimensions",
                "256",
                "--seed",
                "7",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_generates_every_control_scenario_in_shared_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for scenario in [
                "aligned",
                "complementary",
                "dense-sparse-complementary",
                "dense-text-complementary",
                "sparse-text-complementary",
                "dense-conflict",
                "sparse-conflict",
                "text-conflict",
            ]:
                output = root / scenario
                self.run_generate(output, scenario)
                manifest = json.loads((output / "manifest.json").read_text())
                self.assertEqual(manifest["schema_version"], 1)
                self.assertEqual(manifest["documents"], 60)
                self.assertEqual(manifest["queries"], 12)
                self.assertEqual(manifest["scenario"], scenario)
                self.assertEqual(
                    manifest["qrels_semantics"],
                    "shared-across-all-retrieval-modes",
                )
                self.assertTrue(manifest["dense"]["publication_valid"])
                self.assertEqual(
                    manifest["retrieval_modes"][-1],
                    "dense+sparse+text",
                )
                self.assertEqual(
                    (output / "corpus.dense.f32").stat().st_size,
                    60 * 24 * 4,
                )
                self.assertEqual(
                    (output / "queries.dense.f32").stat().st_size,
                    12 * 24 * 4,
                )
                qrels = (output / "qrels.tsv").read_text().splitlines()
                expected_relevant = (
                    36
                    if scenario == "complementary"
                    else 24
                    if scenario.endswith("-complementary")
                    else 12
                )
                self.assertEqual(len(qrels) - 1, expected_relevant)
                self.assertEqual(
                    manifest["relevant_documents_per_query"],
                    expected_relevant // 12,
                )

    def test_pairwise_complementary_controls_expose_the_intended_modalities(
        self,
    ) -> None:
        expected = {
            "dense-sparse-complementary": ["dense", "sparse"],
            "dense-text-complementary": ["dense", "text"],
            "sparse-text-complementary": ["sparse", "text"],
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for scenario, modalities in expected.items():
                output = root / scenario
                self.run_generate(output, scenario)
                manifest = json.loads((output / "manifest.json").read_text())
                self.assertEqual(
                    manifest["complementary_modalities"],
                    modalities,
                )
                self.assertEqual(manifest["document_channels"], len(modalities))
                self.assertEqual(manifest["qrels"], 12 * len(modalities))

    def test_generation_is_byte_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            self.run_generate(first, "complementary")
            self.run_generate(second, "complementary")
            self.assertEqual(
                sorted(path.name for path in first.iterdir()),
                sorted(path.name for path in second.iterdir()),
            )
            for path in first.iterdir():
                self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())

    def test_text_topic_markers_are_tokenizer_safe_and_topic_unique(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "aligned"
            self.run_generate(output, "aligned")
            corpus = [
                json.loads(line)
                for line in (output / "corpus.jsonl").read_text().splitlines()
            ]
            queries = [
                json.loads(line)
                for line in (output / "queries.jsonl").read_text().splitlines()
            ]

            document_markers = [row["text"].split()[0] for row in corpus[::10]]
            query_markers = [row["text"].split()[0] for row in queries[::2]]
            self.assertEqual(document_markers, query_markers)
            self.assertEqual(len(set(document_markers)), 6)
            for marker in document_markers:
                self.assertRegex(marker, re.compile(r"^[a-z]+$"))

    def test_rejects_non_divisible_topic_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--output",
                    str(Path(temporary) / "bad"),
                    "--dataset",
                    "bad",
                    "--documents",
                    "10",
                    "--queries",
                    "6",
                    "--topics",
                    "3",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("divisible", completed.stderr.lower())


if __name__ == "__main__":
    unittest.main()
