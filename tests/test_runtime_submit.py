from __future__ import annotations

import inspect
from typing import Any

from subsystem_announcement.extract import AnnouncementFactCandidate, extract_fact_candidates
from subsystem_announcement.runtime import submit as submit_module
from subsystem_announcement.runtime.submit import (
    SubmitIdempotencyStore,
    submit_candidates,
)

from .extract_fixtures import make_artifact


class RecordingSubsystem:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.submissions: list[dict[str, Any]] = []

    def submit(self, candidate: dict[str, Any]) -> Any:
        self.submissions.append(candidate)
        if not self.outcomes:
            return _accepted(f"receipt-{len(self.submissions)}")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_submit_candidates_preserves_order_and_records_receipts() -> None:
    first = _fact("ann-submit-1")
    second = first.model_copy(update={"fact_id": f"{first.fact_id}:second"})
    subsystem = RecordingSubsystem([_accepted("receipt-1"), _accepted("receipt-2")])

    result = submit_candidates([first, second], subsystem)  # type: ignore[arg-type]

    assert [payload["fact_id"] for payload in subsystem.submissions] == [
        first.fact_id,
        second.fact_id,
    ]
    assert result.submitted == 2
    assert result.skipped_duplicates == 0
    assert result.failed == 0
    assert [receipt.receipt_id for receipt in result.receipts] == [
        "receipt-1",
        "receipt-2",
    ]
    assert [trace.status for trace in result.traces] == ["accepted", "accepted"]


def test_submit_candidates_skips_duplicate_fact_id_and_payload_hash() -> None:
    fact = _fact("ann-duplicate")
    subsystem = RecordingSubsystem([_accepted("receipt-original")])
    store = SubmitIdempotencyStore()

    first = submit_candidates([fact], subsystem, idempotency_store=store)  # type: ignore[arg-type]
    second = submit_candidates([fact], subsystem, idempotency_store=store)  # type: ignore[arg-type]

    assert first.submitted == 1
    assert second.submitted == 0
    assert second.skipped_duplicates == 1
    assert second.failed == 0
    assert len(subsystem.submissions) == 1
    assert second.receipts[0].receipt_id == "receipt-original"
    assert second.traces[0].status == "duplicate"
    assert second.traces[0].receipt_id == "receipt-original"


def test_submit_candidates_retries_transient_exception() -> None:
    fact = _fact("ann-retry")
    subsystem = RecordingSubsystem(
        [RuntimeError("temporary transport failure"), _accepted("receipt-after-retry")]
    )

    result = submit_candidates([fact], subsystem, max_attempts=3)  # type: ignore[arg-type]

    assert result.submitted == 1
    assert result.failed == 0
    assert len(subsystem.submissions) == 2
    assert result.receipts[0].attempts == 2
    assert result.traces[0].attempts == 2


def test_submit_candidates_records_rejected_result_as_failure() -> None:
    fact = _fact("ann-rejected")
    subsystem = RecordingSubsystem(
        [
            _rejected("receipt-rejected-1", "contract rejected"),
            _rejected("receipt-rejected-2", "contract still rejected"),
        ]
    )

    result = submit_candidates([fact], subsystem, max_attempts=2)  # type: ignore[arg-type]

    assert result.submitted == 0
    assert result.failed == 1
    assert len(subsystem.submissions) == 2
    assert result.failures[0].fact_id == fact.fact_id
    assert result.failures[0].attempts == 2
    assert "contract still rejected" in result.failures[0].errors[-1]


def test_submit_candidates_rejects_invalid_payload_before_sdk_call() -> None:
    fact = _fact("ann-invalid")
    invalid = fact.model_copy(update={"evidence_spans": []})
    subsystem = RecordingSubsystem([_accepted("should-not-submit")])

    result = submit_candidates([invalid], subsystem)  # type: ignore[arg-type]

    assert result.submitted == 0
    assert result.failed == 1
    assert subsystem.submissions == []
    assert "evidence span" in result.failures[0].errors[0]


def test_submit_candidates_rejects_runtime_metadata_before_sdk_call() -> None:
    fact = _fact("ann-runtime-metadata")
    invalid = fact.model_copy(
        update={
            "source_reference": {
                **fact.source_reference,
                "local_path": "/tmp/cache.pdf",
            }
        }
    )
    subsystem = RecordingSubsystem([_accepted("should-not-submit")])

    result = submit_candidates([invalid], subsystem)  # type: ignore[arg-type]

    assert result.submitted == 0
    assert result.failed == 1
    assert subsystem.submissions == []
    assert "forbidden" in result.failures[0].errors[0]


def test_runtime_submit_does_not_import_sdk_transport_directly() -> None:
    source = inspect.getsource(submit_module)

    assert "subsystem_sdk.submit" not in source
    assert "from subsystem_sdk" not in source


def _fact(announcement_id: str) -> AnnouncementFactCandidate:
    artifact = make_artifact(
        "证券代码：600000\n证券简称：测试公司\n"
        "公司与华东能源签订重大合同，合同金额为1000万元。",
        announcement_id=announcement_id,
    )
    return extract_fact_candidates(artifact)[0]


def _accepted(receipt_id: str) -> dict[str, Any]:
    return {
        "accepted": True,
        "receipt_id": receipt_id,
        "warnings": (),
        "errors": (),
    }


def _rejected(receipt_id: str, error: str) -> dict[str, Any]:
    return {
        "accepted": False,
        "receipt_id": receipt_id,
        "warnings": (),
        "errors": (error,),
    }
