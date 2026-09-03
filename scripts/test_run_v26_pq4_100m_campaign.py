import unittest

from scripts.run_v26_pq4_100m_campaign import (
    AttemptRegistry,
    CampaignMonitor,
    MonitorLimits,
    MonitorSnapshot,
    S3LatencyProfile,
    S3RequestCounter,
    estimate_s3_transfer,
)


def snapshot(
    elapsed: int,
    *,
    phase: str = "build",
    progress: int = 0,
    rss_bytes: int = 1_000_000,
    psi_full_avg10: float = 0.0,
    swap_bytes: int = 0,
    state: str = "running",
    system_status: str = "ok",
    instance_status: str = "ok",
) -> MonitorSnapshot:
    return MonitorSnapshot(
        elapsed_seconds=elapsed,
        phase=phase,
        progress=progress,
        rss_bytes=rss_bytes,
        psi_full_avg10=psi_full_avg10,
        swap_bytes=swap_bytes,
        instance_state=state,
        system_status=system_status,
        instance_status=instance_status,
    )


class CampaignMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.limits = MonitorLimits(
            build_rss_bytes=12 * 1024**3,
            serve_rss_bytes=3 * 1024**3,
            psi_full_avg10=0.01,
            build_stall_seconds=300,
            serve_stall_seconds=60,
            build_wall_seconds=7_200,
            serve_wall_seconds=3_600,
        )

    def test_v26_pq4_100m_monitor_allows_healthy_progress(self) -> None:
        monitor = CampaignMonitor(self.limits, initial_swap_bytes=0)
        for item in [
            snapshot(0, progress=0),
            snapshot(30, progress=1_000_000),
            snapshot(300, progress=9_000_000),
            snapshot(0, phase="serve", progress=0),
            snapshot(30, phase="serve", progress=40),
        ]:
            decision = monitor.observe(item)
            self.assertEqual(decision.action, "continue", decision)
            self.assertEqual(decision.reason, "healthy")

    def test_v26_pq4_100m_monitor_stops_at_every_registered_health_boundary(self) -> None:
        cases = {
            "build-rss": [snapshot(1, rss_bytes=12 * 1024**3 + 1)],
            "serve-rss": [
                snapshot(1, phase="serve", rss_bytes=3 * 1024**3 + 1)
            ],
            "swap-growth": [snapshot(1, swap_bytes=1)],
            "ec2-system": [snapshot(1, system_status="impaired")],
            "ec2-instance": [snapshot(1, instance_status="impaired")],
            "instance-terminal": [snapshot(1, state="stopped")],
            "build-wall": [snapshot(7_201)],
            "serve-wall": [snapshot(3_601, phase="serve")],
        }
        for expected, observations in cases.items():
            with self.subTest(expected=expected):
                monitor = CampaignMonitor(self.limits, initial_swap_bytes=0)
                decision = None
                for item in observations:
                    decision = monitor.observe(item)
                self.assertEqual(decision.action, "stop")
                self.assertEqual(decision.reason, expected)

    def test_v26_pq4_100m_monitor_requires_three_psi_breaches(self) -> None:
        monitor = CampaignMonitor(self.limits, initial_swap_bytes=0)
        self.assertEqual(monitor.observe(snapshot(1, psi_full_avg10=0.011)).action, "continue")
        self.assertEqual(monitor.observe(snapshot(31, psi_full_avg10=0.012)).action, "continue")
        decision = monitor.observe(snapshot(61, psi_full_avg10=0.013))
        self.assertEqual((decision.action, decision.reason), ("stop", "memory-psi"))

        recovered = CampaignMonitor(self.limits, initial_swap_bytes=0)
        for value in [0.011, 0.012, 0.0, 0.013, 0.014]:
            self.assertEqual(
                recovered.observe(snapshot(1, psi_full_avg10=value)).action,
                "continue",
            )

    def test_v26_pq4_100m_monitor_stops_when_progress_stalls(self) -> None:
        build = CampaignMonitor(self.limits, initial_swap_bytes=0)
        self.assertEqual(build.observe(snapshot(0, progress=10)).action, "continue")
        self.assertEqual(build.observe(snapshot(300, progress=10)).action, "continue")
        decision = build.observe(snapshot(301, progress=10))
        self.assertEqual((decision.action, decision.reason), ("stop", "build-stalled"))

        serve = CampaignMonitor(self.limits, initial_swap_bytes=0)
        self.assertEqual(
            serve.observe(snapshot(0, phase="serve", progress=10)).action, "continue"
        )
        decision = serve.observe(snapshot(61, phase="serve", progress=10))
        self.assertEqual((decision.action, decision.reason), ("stop", "serve-stalled"))


class AttemptRegistryTests(unittest.TestCase):
    def test_v26_pq4_100m_attempt_registry_never_duplicates_live_work(self) -> None:
        registry = AttemptRegistry(range(10))
        registry.start(4, "partition-0004-a0001", "i-0004")
        with self.assertRaises(ValueError):
            registry.start(4, "partition-0004-a0001", "i-0004")
        with self.assertRaises(ValueError):
            registry.start(4, "partition-0004-a0002", "i-0005")
        registry.finish(4, "partition-0004-a0001", "passed")
        with self.assertRaises(ValueError):
            registry.start(4, "partition-0004-a0002", "i-0005")

    def test_v26_pq4_100m_attempt_registry_replaces_only_spot_interruption(self) -> None:
        registry = AttemptRegistry(range(10))
        registry.start(7, "partition-0007-a0001", "i-0007")
        registry.finish(7, "partition-0007-a0001", "spot-interrupted")
        registry.start(7, "partition-0007-a0002", "i-0017")
        self.assertEqual(
            registry.active_attempts(),
            {7: ("partition-0007-a0002", "i-0017")},
        )

        registry.start(8, "partition-0008-a0001", "i-0008")
        registry.finish(8, "partition-0008-a0001", "failed")
        with self.assertRaises(ValueError):
            registry.start(8, "partition-0008-a0002", "i-0018")


class S3LatencyProjectionTests(unittest.TestCase):
    def test_v26_pq4_100m_s3_projection_counts_exact_requests_and_bytes(self) -> None:
        # Break caught: a remote layout reaches a long campaign before anyone accounts for its
        # object GET fan-out or distinguishes request RTT from aggregate transfer throughput.
        counter = S3RequestCounter()
        counter.add("manifest", requests=10, bytes_read=100_000)
        counter.add("snapshot", requests=90, bytes_read=999_900_000)
        self.assertEqual(counter.counts(), (100, 1_000_000_000))

        projection = estimate_s3_transfer(
            counter,
            S3LatencyProfile(
                name="expected",
                request_latency_ms=10,
                aggregate_bytes_per_second=100_000_000,
                parallel_requests=10,
            ),
            wall_budget_seconds=11.0,
        )
        self.assertEqual(projection.request_waves, 10)
        self.assertAlmostEqual(projection.request_seconds, 0.1)
        self.assertAlmostEqual(projection.transfer_seconds, 10.0)
        self.assertAlmostEqual(projection.wall_seconds, 10.1)
        self.assertTrue(projection.within_wall_budget)

        over_budget = estimate_s3_transfer(
            counter,
            S3LatencyProfile("pessimistic", 40, 25_000_000, 4),
            wall_budget_seconds=45.0,
        )
        self.assertEqual(over_budget.request_waves, 25)
        self.assertAlmostEqual(over_budget.wall_seconds, 41.0)
        self.assertTrue(over_budget.within_wall_budget)

        with self.assertRaises(ValueError):
            counter.add("snapshot", requests=0, bytes_read=1)
        with self.assertRaises(ValueError):
            estimate_s3_transfer(counter, projection.profile, wall_budget_seconds=0)


if __name__ == "__main__":
    unittest.main()
