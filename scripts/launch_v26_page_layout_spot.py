#!/usr/bin/env python3
"""Spot-only multi-AZ launcher boundary for V26 layout phases."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any, Protocol


@dataclasses.dataclass(frozen=True)
class V26SpotPlan:
    profile: str
    image_id: str
    instance_type: str
    subnet_ids: tuple[str, ...]
    market_options: dict[str, Any]


class EC2Client(Protocol):
    def run_instances(self, **request: Any) -> dict[str, Any]: ...

    def terminate_instances(self, *, InstanceIds: list[str]) -> object: ...  # noqa: N803


def build_v26_spot_plan(
    *,
    profile: str,
    image_id: str,
    instance_type: str,
    subnet_ids: tuple[str, ...],
) -> V26SpotPlan:
    if profile != "causality" or not image_id or not instance_type or not subnet_ids:
        raise ValueError("V26 Spot plan authority differs")
    if len(set(subnet_ids)) != len(subnet_ids) or any(not value for value in subnet_ids):
        raise ValueError("V26 Spot subnet inventory differs")
    return V26SpotPlan(
        profile=profile,
        image_id=image_id,
        instance_type=instance_type,
        subnet_ids=subnet_ids,
        market_options={
            "MarketType": "spot",
            "SpotOptions": {
                "InstanceInterruptionBehavior": "terminate",
                "SpotInstanceType": "one-time",
            },
        },
    )


def run_v26_spot_phase(
    client: EC2Client,
    plan: V26SpotPlan,
    run_phase: Callable[[str], object],
) -> object:
    instance_id = None
    failures: list[Exception] = []
    for subnet_id in plan.subnet_ids:
        try:
            response = client.run_instances(
                ImageId=plan.image_id,
                InstanceType=plan.instance_type,
                MinCount=1,
                MaxCount=1,
                SubnetId=subnet_id,
                InstanceMarketOptions=plan.market_options,
            )
        except Exception as error:
            failures.append(error)
            continue
        instances = response.get("Instances", [])
        observed_ids = [
            instance["InstanceId"]
            for instance in instances
            if isinstance(instance, dict)
            and isinstance(instance.get("InstanceId"), str)
        ]
        if len(instances) != 1 or len(observed_ids) != 1:
            if observed_ids:
                client.terminate_instances(InstanceIds=observed_ids)
            raise RuntimeError("V26 Spot launch response differs")
        instance_id = observed_ids[0]
        break
    if instance_id is None:
        raise RuntimeError(f"V26 Spot capacity unavailable in {len(failures)} subnets")
    try:
        return run_phase(instance_id)
    finally:
        client.terminate_instances(InstanceIds=[instance_id])
