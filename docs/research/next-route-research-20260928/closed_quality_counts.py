"""Count closed quality receipts; no query execution, fitting or new measurement."""
from pathlib import Path
import hashlib, json
root = Path(__file__).resolve().parents[1]
reports = []
for name in ["relaion/development-score.json", "cohere/development-score.json", "relaion/validation-score.json"]:
    path = root / "native-pipeline-quality-20260928" / name
    raw = path.read_bytes()
    receipt = json.loads(raw)
    samples = receipt["samples"]
    assert len(samples) == receipt["queries"]
    assert [s["query_ordinal"] for s in samples] == list(range(receipt["first"], receipt["first"] + receipt["queries"]))
    assert all(0 <= s["returned_hits"] <= s["fetched_hits"] <= 100 and 0 <= s["flat_hits"] <= 100 for s in samples)
    missing_fetch = sum(100 - s["fetched_hits"] for s in samples)
    fetched_not_returned = sum(s["fetched_hits"] - s["returned_hits"] for s in samples)
    missing_flat = sum(100 - s["flat_hits"] for s in samples)
    deficit = sum(s["flat_hits"] - s["returned_hits"] for s in samples)
    assert missing_fetch + fetched_not_returned - missing_flat == deficit
    assert abs(sum(s["returned_hits"] for s in samples) / len(samples) - receipt["metrics"]["mean_returned_hits"]) < 1e-8
    reports.append(dict(dataset=receipt["dataset"], rows=receipt["rows"], dimensions=receipt["dimensions"], metric=receipt["metric"], k=receipt["k"], first=receipt["first"], queries=len(samples),
        queries_fetching_all_100_gt=sum(s["fetched_hits"] == 100 for s in samples),
        below_flat_with_all_gt_fetched=sum(s["returned_hits"] < s["flat_hits"] and s["fetched_hits"] == 100 for s in samples),
        returned_below_equal_above_flat=[sum(s["returned_hits"] < s["flat_hits"] for s in samples), sum(s["returned_hits"] == s["flat_hits"] for s in samples), sum(s["returned_hits"] > s["flat_hits"] for s in samples)],
        missing_fetch_hits=missing_fetch, fetched_not_returned_hits=fetched_not_returned, missing_flat_hits=missing_flat, net_deficit_hits=deficit,
        input=str(path.relative_to(root)), input_sha256=hashlib.sha256(raw).hexdigest()))
print(json.dumps(dict(scope="Post-hoc closed consumed-panel reconciliation; no discovery/selection separation, new quality measurement, fresh validation or serving performance", baseline="Paired exhaustive native SQ8 over identical rows", reports=reports), indent=2))
