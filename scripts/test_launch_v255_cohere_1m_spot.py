import unittest

from scripts.launch_v248_cohere_transfer_100k_spot import worker


class V255WorkerTest(unittest.TestCase):
    def test_single_million_row_arm_is_frozen(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt", million=True)
        self.assertIn("--rows 1000000 --generation 255", script)
        self.assertIn("for n in $(seq 0 45)", script)
        self.assertIn("graph.bin build.json --diverse", script)
        self.assertIn("borsuk-v255-cohere-diverse-graph-1m-spot-v1", script)
        self.assertNotIn("@@", script)

    def test_hybrid_reuses_authenticated_sources_and_seals_loaded_samples(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt", hybrid=True)
        self.assertIn("--hybrid centroids.f32 offsets.u32 postings.u32 coarse.json", script)
        self.assertIn("loaded-raw.jsonl", script)
        self.assertIn("688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7", script)
        self.assertNotIn("@@", script)

    def test_million_hybrid_keeps_single_arm_and_paired_graph(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt", million_hybrid=True)
        self.assertIn("--rows 1000000 --generation 255", script)
        self.assertIn("--hybrid centroids.f32 offsets.u32 postings.u32 coarse.json", script)
        self.assertIn("dff17235c4b674848d52ae11260445f97fd399039e18119f922aaa82d77309a2", script)
        self.assertIn("loaded-raw.jsonl", script)
        self.assertNotIn("@@", script)

    def test_closed_v257_retry_reuses_hash_verified_artifacts(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt",
                        million_hybrid=True, reuse_v257=True)
        self.assertIn("prior-terminal.json", script)
        self.assertIn("tests::million_row_hybrid_accepts_only_the_diverse_million_graph", script)
        self.assertIn("if [ '1' = 1 ]; then", script)
        self.assertIn("--hybrid centroids.f32 offsets.u32 postings.u32 coarse.json", script)
        self.assertNotIn("@@", script)

    def test_million_exact_reuses_closed_graph_and_seals_loaded_samples(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt",
                        million_exact=True, reuse_v257=True)
        self.assertIn("--million-exact", script)
        self.assertIn("borsuk-v258-cohere-fp16-navigation-1m-spot-v1", script)
        self.assertIn("prior-terminal.json", script)
        self.assertIn("loaded-raw.jsonl", script)
        self.assertIn("62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39", script)
        self.assertNotIn("@@", script)

    def test_dual_navigation_keeps_both_paths_and_seals_loaded_samples(self):
        script = worker("0" * 40, "1" * 64, "source", "attempt", dual=True)
        self.assertIn("--dual centroids.f32 offsets.u32 postings.u32 coarse.json", script)
        self.assertIn("borsuk-v259-cohere-dual-navigation-100k-spot-v1", script)
        self.assertIn("loaded-raw.jsonl", script)
        self.assertIn("688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7", script)
        self.assertNotIn("@@", script)


if __name__ == "__main__":
    unittest.main()
