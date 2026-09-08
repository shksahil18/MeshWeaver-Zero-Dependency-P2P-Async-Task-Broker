"""
Week 3 - Task ledger.

The TaskTracker remembers every task this node has submitted, so the
node can:

    * tell which tasks were running on a peer that just went OFFLINE
    * mark them FAILED
    * re-dispatch the very same function/arguments to another node
    * hand the final result (or final error) to whoever is awaiting it

Task life-cycle
---------------

    PENDING ──dispatch──> DISPATCHED ──TASK_RESULT ok──> COMPLETED
                              │
                              ├── worker raised exception ─────> FAILED (final)
                              │
                              └── node offline / no reply ──> FAILED
                                                              │
                                        attempts left? ── yes ┘──> DISPATCHED (re-routed)
                                                              │
                                                              no ──> FAILED (final)

Every transition is appended to ``TaskRecord.history`` so the full
story of a task ("dispatched to A, A died, re-routed to B, completed")
can be printed afterwards.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


DEFAULT_MAX_ATTEMPTS = 3


class TaskStatus(str, Enum):

    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskFailedError(RuntimeError):
    """
    Raised by ``MeshNode.wait_for_result`` when a task ends FAILED.
    """


@dataclass
class TaskRecord:
    """
    Everything the node knows about one submitted task.
    """

    task_id: str
    function: object
    args: tuple
    kwargs: dict
    payload: bytes | None = None

    status: TaskStatus = TaskStatus.PENDING

    attempts: int = 0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    assigned_to: tuple[str, int] | None = None
    assigned_node_id: str | None = None

    created_at: float = field(default_factory=time.monotonic)
    dispatched_at: float | None = None
    finished_at: float | None = None

    result: object = None
    error: str | None = None

    # Addresses that already failed this task -> excluded on re-route.
    tried: set = field(default_factory=set)

    history: list[str] = field(default_factory=list)

    # Set when the task reaches a final state.
    done: asyncio.Event = field(default_factory=asyncio.Event)

    # --------------------------------------------------------

    @property
    def is_final(self) -> bool:
        return self.done.is_set()

    @property
    def name(self) -> str:
        return getattr(
            self.function,
            "__name__",
            repr(self.function),
        )

    def log(self, text: str):

        elapsed = time.monotonic() - self.created_at

        self.history.append(
            f"+{elapsed:6.2f}s  {text}"
        )

    def describe(self) -> str:

        target = (
            f"{self.assigned_to[0]}:{self.assigned_to[1]}"
            if self.assigned_to
            else "-"
        )

        outcome = ""

        if self.status is TaskStatus.COMPLETED:
            outcome = f"result={self.result!r}"

        elif self.status is TaskStatus.FAILED:
            outcome = f"error={self.error}"

        return (
            f"{self.task_id[:8]}  {self.name:<12} "
            f"{self.status.value:<10} "
            f"attempt {self.attempts}/{self.max_attempts}  "
            f"node={target:<16} {outcome}"
        )


class TaskTracker:
    """
    In-memory registry of submitted tasks.
    """

    def __init__(
        self,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ):
        self.max_attempts = max_attempts
        self.records: dict[str, TaskRecord] = {}

    # ------------------------------------------------------------
    # Creation / lookup
    # ------------------------------------------------------------

    def create(
        self,
        function,
        args=(),
        kwargs=None,
        payload: bytes | None = None,
        max_attempts: int | None = None,
        task_id: str | None = None,
    ) -> TaskRecord:

        record = TaskRecord(
            task_id=task_id or str(uuid.uuid4()),
            function=function,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            payload=payload,
            max_attempts=(
                max_attempts
                if max_attempts is not None
                else self.max_attempts
            ),
        )

        record.log(
            f"PENDING     created ({record.name})"
        )

        self.records[record.task_id] = record

        return record

    def get(self, task_id: str) -> TaskRecord | None:
        return self.records.get(task_id)

    def all(self) -> list[TaskRecord]:
        return list(self.records.values())

    # ------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------

    def mark_dispatched(
        self,
        task_id: str,
        address,
        node_id_hex: str | None = None,
    ) -> TaskRecord:

        record = self._require(task_id)

        address = (
            str(address[0]),
            int(address[1]),
        )

        record.attempts += 1
        record.status = TaskStatus.DISPATCHED
        record.assigned_to = address
        record.assigned_node_id = node_id_hex
        record.dispatched_at = time.monotonic()
        record.tried.add(address)

        record.log(
            f"DISPATCHED  -> {address[0]}:{address[1]} "
            f"(attempt {record.attempts}/{record.max_attempts})"
        )

        return record

    def mark_completed(
        self,
        task_id: str,
        result,
    ) -> TaskRecord:

        record = self._require(task_id)

        record.status = TaskStatus.COMPLETED
        record.result = result
        record.error = None
        record.finished_at = time.monotonic()

        record.log(
            f"COMPLETED   result={result!r}"
        )

        record.done.set()

        return record

    def mark_failed(
        self,
        task_id: str,
        error: str,
        final: bool = True,
    ) -> TaskRecord:
        """
        Mark a task FAILED.

        ``final=False`` records the failure but leaves the task open
        so it can be re-dispatched (used when a node dies).
        """

        record = self._require(task_id)

        record.status = TaskStatus.FAILED
        record.error = error

        record.log(
            f"FAILED      {error}"
            + ("" if final else "  (will re-route)")
        )

        if final:

            record.finished_at = time.monotonic()

            record.done.set()

        return record

    # ------------------------------------------------------------
    # Queries used by fault tolerance
    # ------------------------------------------------------------

    def dispatched_to(self, address) -> list[TaskRecord]:
        """
        Tasks currently running on ``address``.
        """

        address = (
            str(address[0]),
            int(address[1]),
        )

        return [
            record
            for record in self.records.values()
            if record.status is TaskStatus.DISPATCHED
            and record.assigned_to == address
        ]

    def timed_out(
        self,
        timeout: float,
        now: float | None = None,
    ) -> list[TaskRecord]:
        """
        Dispatched tasks that have produced no result for ``timeout``s.
        """

        if now is None:
            now = time.monotonic()

        return [
            record
            for record in self.records.values()
            if record.status is TaskStatus.DISPATCHED
            and record.dispatched_at is not None
            and now - record.dispatched_at > timeout
        ]

    def can_retry(self, task_id: str) -> bool:

        record = self._require(task_id)

        return (
            not record.is_final
            and record.attempts < record.max_attempts
        )

    # ------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------

    def summary(self) -> dict[str, int]:

        counts = {
            status.value: 0
            for status in TaskStatus
        }

        for record in self.records.values():
            counts[record.status.value] += 1

        return counts

    def format_ledger(self) -> str:

        if not self.records:
            return "[TASKS] No tasks submitted."

        lines = ["[TASKS] Task ledger:"]

        for record in self.records.values():
            lines.append(f"  {record.describe()}")

        return "\n".join(lines)

    # ------------------------------------------------------------

    def _require(self, task_id: str) -> TaskRecord:

        record = self.records.get(task_id)

        if record is None:

            raise KeyError(
                f"Unknown task id: {task_id}"
            )

        return record
