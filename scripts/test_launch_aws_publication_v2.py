import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("launch_aws_publication_v2.sh")


class LaunchAwsPublicationV2Tests(unittest.TestCase):
    def test_launcher_is_content_addressed_and_rejects_campaign_contention(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("git ls-files", source)
        self.assertIn("shasum -a 256", source)
        self.assertIn("s3api put-object", source)
        self.assertIn("manifest_sha256", source)
        self.assertIn("EXPECTED_ACCOUNT", source)
        self.assertIn("bench_publication_v2_aws.sh", source)
        self.assertIn("tmux new-session -d", source)
        self.assertIn("tmux list-panes -a", source)
        self.assertIn("another BORSUK campaign is active", source)
        self.assertIn("BORSUK_RUN_PUBLICATION_V2=1", source)
        self.assertIn("describe-instances", source)
        self.assertIn("describe-volumes", source)
        self.assertIn("get-instance-profile", source)
        self.assertIn("simulate-principal-policy", source)
        self.assertIn("s3vectors:ListVectorBuckets", source)
        self.assertIn("s3vectors:CreateVectorBucket", source)
        self.assertIn("s3vectors:PutVectors", source)
        self.assertIn("S3 Vectors IAM preflight failed", source)
        self.assertIn("BORSUK_INSTANCE_TYPE", source)
        self.assertIn("BORSUK_LOCAL_DISK_CLASS", source)
        self.assertIn("BORSUK_ACCELERATOR", source)
        self.assertIn("BORSUK_INDEX_STORAGE_CLASS", source)
        self.assertIn("campaign_id", source)
        self.assertIn("campaign id mismatch", source)
        self.assertIn("result_prefix", source)
        self.assertIn("index_prefix", source)

    def test_inputs_are_chunked_through_ssm_and_verified_before_remote_upload(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn("SSM_CHUNK_BYTES", source)
        self.assertIn("SSM_MAX_IN_FLIGHT", source)
        self.assertIn("stage_file_through_ssm", source)
        self.assertIn("wait_for_ssm_batch", source)
        self.assertIn("split -b", source)
        self.assertIn("base64 -d", source)
        self.assertIn("sha256sum", source)
        self.assertIn("remote source digest mismatch", source)
        self.assertIn("remote manifest digest mismatch", source)
        self.assertIn("aws s3api put-object", source)
        self.assertNotIn(
            'aws --profile "$PROFILE" --region "$REGION" s3api put-object',
            source,
        )

    def test_contention_guard_ignores_dead_retained_tmux_panes(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn(
            'tmux list-panes -a -F "#{session_name} #{pane_dead}"',
            source,
        )
        self.assertIn("only live panes participate in contention", source)
        self.assertIn("tmux kill-session -t \"$session\"", source)
        self.assertNotIn('tmux list-sessions -F "#S"', source)


if __name__ == "__main__":
    unittest.main()
