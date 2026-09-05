"""Opt-in, serial Rust/PyArrow proof for the bounded code-object format."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(
    os.environ.get("BORSUK_CODE_OBJECT_INTERCHANGE") == "1",
    "explicit Rust interchange gate only",
)
class CodeObjectInterchangeTests(unittest.TestCase):
    def test_independent_python_and_rust_objects(self):
        import numpy as np
        import pyarrow as pa

        centroid_type = pa.list_(pa.field("element", pa.float16(), nullable=False), 96)
        range_type = pa.list_(
            pa.field(
                "element",
                pa.struct(
                    [
                        pa.field("logical_start", pa.uint64(), nullable=False),
                        pa.field("row_count", pa.uint32(), nullable=False),
                    ]
                ),
                nullable=False,
            )
        )
        schema = pa.schema(
            [
                pa.field("code_parent_ordinal", pa.uint32(), nullable=False),
                pa.field("centroid", centroid_type, nullable=False),
                pa.field("ranges", range_type, nullable=False),
                pa.field("high_bits", pa.binary(), nullable=False),
                pa.field("base_codes", pa.binary(), nullable=False),
                pa.field("high_codes", pa.binary(), nullable=False),
            ],
            metadata={b"borsuk.format": b"borsuk-v32-bounded-code-object-v1"},
        )
        bits = np.zeros(96, dtype=np.uint16)
        bits[0], bits[1] = 0x8000, 1
        centroid = bits.view(np.float16)
        batch = pa.record_batch(
            [
                pa.array([7], type=pa.uint32()),
                pa.array([centroid], type=centroid_type),
                pa.array(
                    [
                        [
                            dict(logical_start=99, row_count=2),
                            dict(logical_start=200, row_count=2),
                        ]
                    ],
                    type=range_type,
                ),
                pa.array([b"\x0a"], type=pa.binary()),
                pa.array([bytes([1]) * 24 + bytes([3]) * 24], type=pa.binary()),
                pa.array([bytes([2]) * 48 + bytes([4]) * 48], type=pa.binary()),
            ],
            schema=schema,
        )
        with tempfile.TemporaryDirectory(
            prefix="borsuk-code-object-interchange-"
        ) as scratch:
            directory = Path(scratch)
            with pa.OSFile(str(directory / "python.arrow"), "wb") as sink:
                with pa.ipc.new_file(sink, schema) as writer:
                    writer.write_batch(batch)
            command = [
                "rtk",
                "proxy",
                "cargo",
                "test",
                "-p",
                "borsuk",
                "--lib",
                "v32_code_objects::tests::v32_code_object_python_interchange",
                "--",
                "--exact",
                "--ignored",
                "--nocapture",
            ]
            result = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=dict(
                    os.environ,
                    CARGO_BUILD_JOBS="1",
                    BORSUK_CODE_OBJECT_FIXTURE_DIR=scratch,
                ),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=300,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout[-6000:])
            self.assertIn("1 passed; 0 failed", result.stdout)
            with pa.memory_map(str(directory / "rust.arrow"), "r") as source:
                reader = pa.ipc.open_file(source)
                self.assertEqual(reader.num_record_batches, 1)
                self.assertTrue(reader.schema.equals(schema, check_metadata=True))
                output = reader.get_batch(0)
                self.assertEqual(output.column(0).to_pylist(), [0])
                self.assertEqual(
                    output.column(2).to_pylist(),
                    [
                        [
                            dict(logical_start=10, row_count=2),
                            dict(logical_start=20, row_count=2),
                        ]
                    ],
                )
                self.assertEqual(output.column(3).to_pylist(), [b"\x0a"])
                self.assertEqual(
                    output.column(4).to_pylist(), [bytes([1]) * 24 + bytes([3]) * 24]
                )
                self.assertEqual(
                    output.column(5).to_pylist(), [bytes([2]) * 48 + bytes([4]) * 48]
                )
                actual_bits = output.column(1).values.to_numpy().view(np.uint16)
                np.testing.assert_array_equal(actual_bits, bits)
        self.assertFalse(directory.exists())


if __name__ == "__main__":
    unittest.main()
