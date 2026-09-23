"""Terminal-bound transfer floor certificate checks."""

from __future__ import annotations

import hashlib
import json
import unittest

from scripts.certify_native_progressive_transfer_floor import certify_plans


def _canonical(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _plan(*, sign: int, magnitude: int, data: int, ordinal: int) -> tuple[dict, dict]:
    code = {
        "query_ordinal": ordinal,
        "code": {
            plane: {
                "encoded_bytes": length, "gets": 1,
                "included_pages": [["base", 0]], "ranges": [["base", 0, 0]],
            }
            for plane, length in (("sign", sign), ("magnitude", magnitude))
        },
    }
    paired = {
        "query_ordinal": ordinal,
        "two_bit": {"encoded_bytes": data, "gets": 1},
    }
    return code, paired


def _fixture() -> tuple[bytes, bytes, bytes, bytes]:
    cases = [_plan(sign=104, magnitude=96, data=780, ordinal=0),
             _plan(sign=208, magnitude=192, data=1560, ordinal=1)]
    code = _canonical({
        "schema": "borsuk-one-million-progressive-mirrored-code-projection-v1-plans",
        "samples": [case[0] for case in cases],
    })
    paired = _canonical({
        "schema": "borsuk-one-million-progressive-paired-score-v1-plans",
        "prior_plans_sha256": _sha(code),
        "samples": [case[1] for case in cases],
    })
    code_terminal = _canonical({
        "status": "complete", "exit_code": 0,
        "artifacts": {"progressive-code-wave-plans": {"sha256": _sha(code),
                       "encoded_bytes": len(code)}},
    })
    paired_terminal = _canonical({
        "status": "complete", "exit_code": 0,
        "artifacts": {"progressive-paired-plans": {"sha256": _sha(paired),
                       "encoded_bytes": len(paired)}},
    })
    return code, paired, code_terminal, paired_terminal


class TransferFloorTests(unittest.TestCase):
    def test_terminal_bound_minima_and_query_alignment(self) -> None:
        bodies = _fixture()
        result = certify_plans(*bodies, expected_queries=2,
                               code_terminal_sha256=_sha(bodies[2]),
                               paired_terminal_sha256=_sha(bodies[3]))
        self.assertEqual(result["minimum_bytes"], {
            "sign": 104, "magnitude": 96, "data": 780, "total": 980,
        })
        self.assertEqual(result["maximum_gets"], 3)
        self.assertEqual(result["query_count"], 2)

    def test_rejects_terminal_or_plan_tampering(self) -> None:
        code, paired, code_terminal, paired_terminal = _fixture()
        with self.assertRaisesRegex(ValueError, "terminal identity"):
            certify_plans(code, paired, code_terminal, paired_terminal,
                          expected_queries=2, code_terminal_sha256="0" * 64,
                          paired_terminal_sha256=_sha(paired_terminal))
        changed = paired.replace(b'"encoded_bytes":780', b'"encoded_bytes":781')
        with self.assertRaisesRegex(ValueError, "artifact identity"):
            certify_plans(code, changed, code_terminal, paired_terminal,
                          expected_queries=2,
                          code_terminal_sha256=_sha(code_terminal),
                          paired_terminal_sha256=_sha(paired_terminal))

        malformed = _canonical({"status": "complete", "exit_code": 0,
                                "artifacts": []})
        with self.assertRaisesRegex(ValueError, "artifact identity"):
            certify_plans(code, paired, malformed, paired_terminal,
                          expected_queries=2,
                          code_terminal_sha256=_sha(malformed),
                          paired_terminal_sha256=_sha(paired_terminal))

    def test_rejects_missing_mirrored_cover(self) -> None:
        code, paired, code_terminal, paired_terminal = _fixture()
        value = json.loads(code)
        value["samples"][0]["code"]["magnitude"]["included_pages"] = [["base", 1]]
        changed = _canonical(value)
        receipt = json.loads(code_terminal)
        receipt["artifacts"]["progressive-code-wave-plans"] = {
            "sha256": _sha(changed), "encoded_bytes": len(changed),
        }
        changed_terminal = _canonical(receipt)
        paired_value = json.loads(paired)
        paired_value["prior_plans_sha256"] = _sha(changed)
        changed_paired = _canonical(paired_value)
        paired_receipt = json.loads(paired_terminal)
        paired_receipt["artifacts"]["progressive-paired-plans"] = {
            "sha256": _sha(changed_paired), "encoded_bytes": len(changed_paired),
        }
        changed_paired_terminal = _canonical(paired_receipt)
        with self.assertRaisesRegex(ValueError, "mirrored cover"):
            certify_plans(changed, changed_paired, changed_terminal,
                          changed_paired_terminal, expected_queries=2,
                          code_terminal_sha256=_sha(changed_terminal),
                          paired_terminal_sha256=_sha(changed_paired_terminal))
