import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.publication_v3_aws import build_staging_receipt, staging_jobs

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/research/publication-v3-manifest.json"
SCRIPT = ROOT / "scripts/reconcile_publication_v3_staging.py"


class ReconcilePublicationV3StagingTests(unittest.TestCase):
    def test_stopped_attempt_is_observed_then_terminated_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            log = temp / "aws.log"
            fake = temp / "aws"
            fake.write_text(
                """#!/usr/bin/env python3
import json, os, pathlib, sys
pathlib.Path(os.environ['FAKE_AWS_LOG']).open('a').write(json.dumps(sys.argv[1:]) + '\\n')
if 'describe-instances' in sys.argv:
    campaign = 'wrong-campaign' if os.environ.get('FAKE_BAD_TAG') else 'publication-v3-20260812'
    print(json.dumps({'Reservations':[{'Instances':[{
        'State':{'Name':'stopped'},
        'InstanceLifecycle':'spot',
        'Tags':[
            {'Key':'Project','Value':'BorsukBenchmark'},
            {'Key':'Campaign','Value':campaign},
            {'Key':'Cell','Value':'stage-sift-128'},
            {'Key':'Attempt','Value':'2'},
            {'Key':'Role','Value':'staging'},
            {'Key':'AutoTerminate','Value':'true'},
        ],
    }]}]}))
elif 'head-object' in sys.argv:
    if os.environ.get('FAKE_HEAD_MODE') == 'denied':
        print('An error occurred (403) when calling HeadObject: AccessDenied', file=sys.stderr)
    else:
        print('An error occurred (404) when calling HeadObject: Not Found', file=sys.stderr)
    raise SystemExit(255)
elif 'terminate-instances' in sys.argv:
    print('{}')
else:
    raise SystemExit(96)
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(MANIFEST),
                    "--dataset",
                    "sift-128",
                    "--attempt",
                    "2",
                    "--instance-id",
                    "i-0123456789abcdef0",
                    "--profile",
                    "causality",
                    "--region",
                    "eu-central-1",
                    "--max-attempts",
                    "3",
                    "--aws-command",
                    str(fake),
                ],
                cwd=ROOT,
                env={**os.environ, "FAKE_AWS_LOG": str(log)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            value = json.loads(completed.stdout)
            self.assertEqual(value["action"], "retry-fresh-attempt")
            self.assertEqual(value["next_job"]["attempt"], 3)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(sum("describe-instances" in call for call in calls), 1)
            self.assertEqual(sum("head-object" in call for call in calls), 2)
            self.assertEqual(sum("terminate-instances" in call for call in calls), 1)
            self.assertFalse(any("get-object" in call for call in calls))

            log.write_text("")
            denied = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(MANIFEST),
                    "--dataset",
                    "sift-128",
                    "--attempt",
                    "2",
                    "--instance-id",
                    "i-0123456789abcdef0",
                    "--profile",
                    "causality",
                    "--region",
                    "eu-central-1",
                    "--aws-command",
                    str(fake),
                ],
                cwd=ROOT,
                env={
                    **os.environ,
                    "FAKE_AWS_LOG": str(log),
                    "FAKE_HEAD_MODE": "denied",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(denied.returncode, 2)
            self.assertIn("AccessDenied", denied.stderr)
            denied_calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertFalse(any("terminate-instances" in call for call in denied_calls))

            log.write_text("")
            wrong_instance = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(MANIFEST),
                    "--dataset",
                    "sift-128",
                    "--attempt",
                    "2",
                    "--instance-id",
                    "i-0123456789abcdef0",
                    "--region",
                    "eu-central-1",
                    "--aws-command",
                    str(fake),
                ],
                cwd=ROOT,
                env={
                    **os.environ,
                    "FAKE_AWS_LOG": str(log),
                    "FAKE_BAD_TAG": "1",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(wrong_instance.returncode, 2)
            self.assertIn("instance identity", wrong_instance.stderr)
            wrong_calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertFalse(any("head-object" in call for call in wrong_calls))
            self.assertFalse(any("terminate-instances" in call for call in wrong_calls))

    def test_complete_attempt_downloads_validated_receipt_then_terminates(self) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        job = next(
            job for job in staging_jobs(manifest) if job.dataset_id == "sift-128"
        )
        objects = tuple(
            {
                "role": role,
                "format": "json" if role == "metadata" else "parquet",
                "uri": f"{job.output_uri}/{name}",
                "sha256": f"{index + 1:064x}",
                "bytes": 1024,
                "rows": rows,
            }
            for index, (role, name, rows) in enumerate(
                (
                    ("train", "train-00000000.parquet", 10),
                    ("query", "test.parquet", 2),
                    ("ground-truth", "neighbors.parquet", 2),
                    ("metadata", "meta.json", 1),
                )
            )
        )
        receipt = build_staging_receipt(
            manifest,
            job,
            source_archive_sha256="a" * 64,
            source_provenance={
                "schema_version": 1,
                "dataset": "sift-128",
                "source": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
                "source_sha256": "b" * 64,
                "materialization_sha256": "c66ceeb981504f9de03a84700e3ef410b3298f67dd92a3768a8cab6de4b2c3ee",
            },
            provenance_sha256="d" * 64,
            objects=objects,
            instance_id="i-0123456789abcdef0",
            instance_type="r7g.8xlarge",
            availability_zone="eu-central-1a",
            purchase_option="spot",
        )
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            receipt_path = temp / "receipt.json"
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            log = temp / "aws.log"
            fake = temp / "aws"
            fake.write_text(
                """#!/usr/bin/env python3
import base64, hashlib, json, os, pathlib, shutil, sys
pathlib.Path(os.environ['FAKE_AWS_LOG']).open('a').write(json.dumps(sys.argv[1:]) + '\\n')
if 'describe-instances' in sys.argv:
    print(json.dumps({'Reservations':[{'Instances':[{
        'State':{'Name':'running'}, 'InstanceLifecycle':'spot',
        'Tags':[{'Key':k,'Value':v} for k,v in {
            'Project':'BorsukBenchmark','Campaign':'publication-v3-20260812',
            'Cell':'stage-sift-128','Attempt':'1','Role':'staging','AutoTerminate':'true'
        }.items()]}]}]}))
elif 'head-object' in sys.argv:
    if any('STAGING_COMPLETE.json' in value for value in sys.argv):
        data = pathlib.Path(os.environ['FAKE_RECEIPT']).read_bytes()
        digest = hashlib.sha256(data).digest()
        if os.environ.get('FAKE_BAD_CHECKSUM'): digest = bytes(32)
        print(json.dumps({
            'ContentLength':len(data),
            'Metadata':{'borsuk-sha256':digest.hex()},
            'ChecksumSHA256':base64.b64encode(digest).decode('ascii'),
        }))
    else:
        print('An error occurred (404) when calling HeadObject: Not Found', file=sys.stderr)
        raise SystemExit(255)
elif 'get-object' in sys.argv:
    shutil.copyfile(os.environ['FAKE_RECEIPT'], sys.argv[-1])
    print('{}')
elif 'terminate-instances' in sys.argv: print('{}')
else: raise SystemExit(96)
""",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(MANIFEST),
                    "--dataset",
                    "sift-128",
                    "--attempt",
                    "1",
                    "--instance-id",
                    "i-0123456789abcdef0",
                    "--region",
                    "eu-central-1",
                    "--aws-command",
                    str(fake),
                ],
                cwd=ROOT,
                env={
                    **os.environ,
                    "FAKE_AWS_LOG": str(log),
                    "FAKE_RECEIPT": str(receipt_path),
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout)["action"], "terminate-success")
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(sum("get-object" in call for call in calls), 1)
            self.assertEqual(sum("terminate-instances" in call for call in calls), 1)

            log.write_text("")
            bad_checksum = subprocess.run(
                completed.args,
                cwd=ROOT,
                env={
                    **os.environ,
                    "FAKE_AWS_LOG": str(log),
                    "FAKE_RECEIPT": str(receipt_path),
                    "FAKE_BAD_CHECKSUM": "1",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(bad_checksum.returncode, 2)
            self.assertIn("checksum", bad_checksum.stderr)
            bad_calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertFalse(any("terminate-instances" in call for call in bad_calls))


if __name__ == "__main__":
    unittest.main()
