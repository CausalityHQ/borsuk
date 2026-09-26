"""Small authority and row-order check for the CoHere transfer preparation."""

import argparse
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.v248_prepare_cohere_graph import prepare


class PreparationTest(unittest.TestCase):
    def test_canonical_source_prefix_and_plane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = np.arange(4 * 768, dtype=np.float32).reshape(4, 768) + 1
            shard = root / "train-00000000.parquet"
            pq.write_table(pa.table({"emb": pa.FixedSizeListArray.from_arrays(
                pa.array(data.ravel()), 768)}), shard)
            digest = hashlib.sha256(shard.read_bytes()).hexdigest()
            objects = [{"role": "train", "uri": f"s3://test/train-{i:08d}.parquet",
                        "rows": 4, "bytes": shard.stat().st_size, "sha256": digest}
                       for i in range(458)]
            receipt = root / "receipt.json"
            receipt.write_text(json.dumps({
                "dataset_id": "cohere-large-10m-768",
                "dataset_content_sha256":
                    "fa8ccb38e5c761388e0c2ac211cc219438cd79e802e69debd6197d74f83f11ad",
                "object_count": len(objects), "objects": objects}))
            args = argparse.Namespace(receipt=receipt,
                receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),
                train=[shard], rows=4, generation=248, output=root / "out")
            prepare(args)
            prep = json.loads((args.output / "prep.json").read_text())
            self.assertEqual(prep["source_sha256"],
                             hashlib.sha256(data.astype("<f4").tobytes()).hexdigest())
            self.assertEqual((args.output / "plane.bin").stat().st_size, 64 + 4 * 1544)
            args.output = root / "bad"
            args.train = [root / "train-00000001.parquet"]
            with self.assertRaisesRegex(ValueError, "canonical prefix"):
                prepare(args)


if __name__ == "__main__":
    unittest.main()
