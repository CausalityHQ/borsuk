import json
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prepare_hybrid_dataset.py"


def read_u64(path: Path) -> list[int]:
    payload = path.read_bytes()
    return [value[0] for value in struct.iter_unpack("<Q", payload)]


class PrepareHybridDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "beir"
        (self.source / "qrels").mkdir(parents=True)
        (self.source / "corpus.jsonl").write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "_id": "d1",
                            "title": "Alpha",
                            "text": "apple apple orchard",
                        }
                    ),
                    json.dumps(
                        {
                            "_id": "d2",
                            "title": "Beta",
                            "text": "banana yellow fruit",
                        }
                    ),
                    json.dumps(
                        {
                            "_id": "d3",
                            "title": "",
                            "text": "apple banana salad",
                        }
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.source / "queries.jsonl").write_text(
            "\n".join(
                [
                    json.dumps({"_id": "q1", "text": "apple fruit"}),
                    json.dumps({"_id": "q2", "text": "banana"}),
                    json.dumps({"_id": "unused", "text": "not in qrels"}),
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (self.source / "qrels" / "test.tsv").write_text(
            "query-id\tcorpus-id\tscore\nq1\td1\t2\nq1\td3\t1\nq2\td2\t1\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def run_prepare(self, output: Path) -> None:
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--source",
                str(self.source),
                "--output",
                str(output),
                "--dataset",
                "fixture",
                "--split",
                "test",
                "--dense-backend",
                "hash",
                "--dense-dimensions",
                "16",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_writes_shared_qrels_multimodal_contract(self) -> None:
        output = self.root / "prepared"
        self.run_prepare(output)

        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["dataset"], "fixture")
        self.assertEqual(manifest["split"], "test")
        self.assertEqual(manifest["documents"], 3)
        self.assertEqual(manifest["queries"], 2)
        self.assertEqual(manifest["qrels"], 3)
        self.assertEqual(manifest["dense"]["dimensions"], 16)
        self.assertEqual(manifest["dense"]["backend"], "deterministic-hash")
        self.assertFalse(manifest["dense"]["publication_valid"])
        self.assertEqual(manifest["sparse"]["backend"], "tf-idf")
        self.assertEqual(
            manifest["retrieval_modes"],
            [
                "dense",
                "sparse",
                "text",
                "dense+sparse",
                "dense+text",
                "sparse+text",
                "dense+sparse+text",
            ],
        )

        corpus_rows = [
            json.loads(line)
            for line in (output / "corpus.jsonl").read_text().splitlines()
        ]
        query_rows = [
            json.loads(line)
            for line in (output / "queries.jsonl").read_text().splitlines()
        ]
        self.assertEqual([row["id"] for row in corpus_rows], ["d1", "d2", "d3"])
        self.assertEqual([row["id"] for row in query_rows], ["q1", "q2"])
        self.assertEqual(
            (output / "qrels.tsv").read_text().splitlines(),
            [
                "query-id\tcorpus-id\tscore",
                "q1\td1\t2",
                "q1\td3\t1",
                "q2\td2\t1",
            ],
        )

        self.assertEqual((output / "corpus.dense.f32").stat().st_size, 3 * 16 * 4)
        self.assertEqual((output / "queries.dense.f32").stat().st_size, 2 * 16 * 4)
        corpus_offsets = read_u64(output / "corpus.sparse.offsets.u64")
        query_offsets = read_u64(output / "queries.sparse.offsets.u64")
        self.assertEqual(len(corpus_offsets), 4)
        self.assertEqual(len(query_offsets), 3)
        self.assertEqual(corpus_offsets[0], 0)
        self.assertEqual(query_offsets[0], 0)
        self.assertEqual(
            (output / "corpus.sparse.indices.u32").stat().st_size,
            corpus_offsets[-1] * 4,
        )
        self.assertEqual(
            (output / "queries.sparse.values.f32").stat().st_size,
            query_offsets[-1] * 4,
        )

    def test_output_is_byte_deterministic(self) -> None:
        first = self.root / "first"
        second = self.root / "second"
        self.run_prepare(first)
        self.run_prepare(second)

        first_files = sorted(path.name for path in first.iterdir())
        second_files = sorted(path.name for path in second.iterdir())
        self.assertEqual(first_files, second_files)
        for name in first_files:
            self.assertEqual(
                (first / name).read_bytes(),
                (second / name).read_bytes(),
                name,
            )

    def test_publication_mode_rejects_hash_dense_vectors(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--source",
                str(self.source),
                "--output",
                str(self.root / "bad"),
                "--dataset",
                "fixture",
                "--dense-backend",
                "hash",
                "--publication",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("publication", completed.stderr.lower())

    def test_dense_query_prefix_is_recorded_and_does_not_change_documents(self) -> None:
        baseline = self.root / "baseline"
        prefixed = self.root / "prefixed"
        self.run_prepare(baseline)
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--source",
                str(self.source),
                "--output",
                str(prefixed),
                "--dataset",
                "fixture",
                "--split",
                "test",
                "--dense-backend",
                "hash",
                "--dense-dimensions",
                "16",
                "--dense-query-prefix",
                "Represent this query: ",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        manifest = json.loads((prefixed / "manifest.json").read_text())
        self.assertEqual(manifest["dense"]["query_prefix"], "Represent this query: ")
        self.assertEqual(manifest["dense"]["document_prefix"], "")
        self.assertEqual(
            (baseline / "corpus.dense.f32").read_bytes(),
            (prefixed / "corpus.dense.f32").read_bytes(),
        )
        self.assertNotEqual(
            (baseline / "queries.dense.f32").read_bytes(),
            (prefixed / "queries.dense.f32").read_bytes(),
        )


if __name__ == "__main__":
    unittest.main()
