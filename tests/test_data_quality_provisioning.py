from __future__ import annotations

import time
from threading import Event

from app.services.data_quality_provisioning import DataQualityProvisioner


def test_trigger_runs_metadata_when_params_available() -> None:
    calls: list[object] = []

    def record_metadata(params: object) -> None:
        calls.append(params)

    provisioner = DataQualityProvisioner(params_resolver=lambda: "params", metadata_fn=record_metadata)
    provisioner.trigger(wait=True, reason="unit-test")
    provisioner.shutdown()

    assert calls == ["params"]


def test_trigger_skips_when_params_missing() -> None:
    def missing_params() -> object:
        raise RuntimeError("missing Databricks config")

    provisioner = DataQualityProvisioner(params_resolver=missing_params, metadata_fn=lambda _: None)

    # Should not raise even though the resolver fails.
    provisioner.trigger(wait=True, reason="missing-config")
    provisioner.shutdown()


def test_trigger_coalesces_concurrent_requests() -> None:
    params = object()
    calls: list[object] = []
    started = Event()
    release = Event()

    def blocking_metadata(resolved: object) -> None:
        started.set()
        release.wait(timeout=1.0)
        calls.append(resolved)

    provisioner = DataQualityProvisioner(params_resolver=lambda: params, metadata_fn=blocking_metadata)

    provisioner.trigger(reason="first")
    assert started.wait(timeout=1.0)

    # Schedule a second trigger while the first run is still executing; this should queue another run.
    provisioner.trigger(reason="second")
    release.set()

    for _ in range(50):
        if len(calls) >= 2:
            break
        time.sleep(0.05)

    provisioner.shutdown()

    assert len(calls) == 2
    assert all(call is params for call in calls)
