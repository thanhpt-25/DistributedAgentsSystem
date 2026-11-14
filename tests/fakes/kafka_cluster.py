"""An in-memory Kafka for the transport tests (das-scale-resilience task 10.3; Phase 7 Req 3, 6.2).

Not a mock of the adapter's calls but a small cluster with the behaviour the adapter depends on, in the shape of the
``aiokafka`` clients it replaces (``KafkaFactory``):

* topics with partitions, appended in order, spread round-robin by the producer;
* consumer groups with an *eager* rebalance, like aiokafka's default assignor: whenever a member joins or leaves, every
  member's partitions are revoked (the rebalance listener runs), then re-assigned, and each partition resumes from the
  group's committed offset, not from where the consumer had read;
* ``commit`` from a consumer that no longer owns the partition raises ``CommitFailedError``;
* a client can be "down": its producer, admin and consumers raise ``KafkaConnectionError`` until it is back.
"""

from __future__ import annotations

import asyncio
import itertools
import re
from dataclasses import dataclass, field
from typing import Any

from aiokafka.errors import CommitFailedError, InvalidTopicError, KafkaConnectionError, TopicAlreadyExistsError
from aiokafka.structs import TopicPartition


@dataclass(frozen=True, slots=True)
class Record:
    topic: str
    partition: int
    offset: int
    value: bytes
    headers: tuple[tuple[str, bytes], ...] = ()
    key: bytes | None = None


@dataclass(slots=True)
class _Group:
    members: list[FakeConsumer] = field(default_factory=list)
    committed: dict[TopicPartition, int] = field(default_factory=dict)


class FakeCluster:
    """The shared state; every transport gets its own ``factory(client_id)``."""

    def __init__(self, partitions: int = 3) -> None:
        self.default_partitions = partitions
        self.topics: dict[str, list[list[Record]]] = {}
        self.groups: dict[str, _Group] = {}
        self.down: set[str] = set()
        self.created: list[tuple[str, int]] = []
        self.replication: dict[str, int] = {}
        self._rr: dict[str, itertools.count[int]] = {}

    def factory(self, client_id: str) -> FakeFactory:
        return FakeFactory(self, client_id)

    # -------------------------------------------------------------- inspection (for tests)

    def records(self, topic: str) -> list[Record]:
        return sorted((r for part in self.topics.get(topic, []) for r in part), key=lambda r: (r.partition, r.offset))

    def committed(self, group: str, topic: str, partition: int = 0) -> int | None:
        return self.groups.get(group, _Group()).committed.get(TopicPartition(topic, partition))

    # -------------------------------------------------------------- the cluster's own operations

    def check_up(self, client_id: str) -> None:
        if client_id in self.down:
            raise KafkaConnectionError(f"{client_id} cannot reach the cluster")

    @staticmethod
    def check_name(name: str) -> None:
        """What a real broker enforces: letters, digits, ``.``, ``_`` and ``-``, at most 249 characters."""
        if re.fullmatch(r"[A-Za-z0-9._-]{1,249}", name) is None or name in (".", ".."):
            raise InvalidTopicError(f"{name!r} is not a valid topic name")

    def create_topic(self, name: str, partitions: int | None = None) -> None:
        self.check_name(name)
        self.topics[name] = [[] for _ in range(partitions or self.default_partitions)]
        self.created.append((name, len(self.topics[name])))

    def append(self, topic: str, value: bytes, headers: list[tuple[str, bytes]], key: bytes | None) -> Record:
        self.check_name(topic)
        if topic not in self.topics:
            self.create_topic(topic)
        counter = self._rr.setdefault(topic, itertools.count())
        partition = next(counter) % len(self.topics[topic])
        log = self.topics[topic][partition]
        record = Record(topic, partition, len(log), value, tuple(headers), key)
        log.append(record)
        return record

    async def rebalance(self, group_id: str) -> None:
        """Eager: revoke everything everywhere, then assign again and reset positions to the committed offsets."""
        group = self.groups[group_id]
        for member in group.members:
            old = sorted(member.assignment, key=lambda tp: (tp.topic, tp.partition))
            if old and member.listener is not None:
                await member.listener.on_partitions_revoked(old)
            member.assignment = []
        for topic in sorted({t for m in group.members for t in m.topics}):
            subscribed = [m for m in group.members if topic in m.topics]
            for index in range(len(self.topics.get(topic, []))):
                subscribed[index % len(subscribed)].assignment.append(TopicPartition(topic, index))
        for member in group.members:
            for tp in member.assignment:
                member.position[tp] = group.committed.get(tp, 0 if member.from_start else self.end(tp))
            if member.listener is not None:
                await member.listener.on_partitions_assigned(list(member.assignment))

    def end(self, tp: TopicPartition) -> int:
        return len(self.topics.get(tp.topic, [[]])[tp.partition])


class FakeProducer:
    def __init__(self, cluster: FakeCluster, client_id: str) -> None:
        self._cluster, self._client = cluster, client_id

    async def start(self) -> None:
        self._cluster.check_up(self._client)

    async def stop(self) -> None:
        return None

    async def send_and_wait(
        self, topic: str, value: bytes | None = None, key: bytes | None = None, headers: Any = None, **_: Any
    ) -> Record:
        self._cluster.check_up(self._client)
        return self._cluster.append(topic, value or b"", list(headers or []), key)


class FakeAdmin:
    def __init__(self, cluster: FakeCluster, client_id: str) -> None:
        self._cluster, self._client = cluster, client_id

    async def start(self) -> None:
        self._cluster.check_up(self._client)

    async def close(self) -> None:
        return None

    async def list_topics(self) -> list[str]:
        self._cluster.check_up(self._client)
        return sorted(self._cluster.topics)

    async def create_topics(self, new_topics: list[Any], **_: Any) -> None:
        self._cluster.check_up(self._client)
        for topic in new_topics:
            if topic.name in self._cluster.topics:
                raise TopicAlreadyExistsError()
            self._cluster.create_topic(topic.name, topic.num_partitions)
            self._cluster.replication[topic.name] = topic.replication_factor

    async def delete_topics(self, names: list[str], **_: Any) -> None:
        self._cluster.check_up(self._client)
        for name in names:
            self._cluster.topics.pop(name, None)


class FakeConsumer:
    def __init__(self, cluster: FakeCluster, client_id: str, group_id: str | None, from_start: bool) -> None:
        self._cluster, self._client, self.group_id, self.from_start = cluster, client_id, group_id, from_start
        self.topics: list[str] = []
        self.listener: Any = None
        self.assignment: list[TopicPartition] = []
        self.position: dict[TopicPartition, int] = {}
        self.started = False

    def subscribe(self, topics: Any = (), pattern: Any = None, listener: Any = None) -> None:
        self.topics, self.listener = list(topics), listener

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assignment = list(partitions)

    async def seek_to_end(self, *partitions: TopicPartition) -> None:
        for tp in partitions:
            self.position[tp] = self._cluster.end(tp)

    async def start(self) -> None:
        self._cluster.check_up(self._client)
        self.started = True
        if self.group_id is not None:
            self._cluster.groups.setdefault(self.group_id, _Group()).members.append(self)
            await self._cluster.rebalance(self.group_id)

    async def stop(self) -> None:
        if not self.started:
            return
        self.started = False
        if self.group_id is not None:
            group = self._cluster.groups[self.group_id]
            group.members.remove(self)
            self.assignment = []
            await self._cluster.rebalance(self.group_id)

    async def getmany(
        self, *partitions: Any, timeout_ms: int = 0, max_records: int | None = None
    ) -> dict[Any, list[Record]]:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while True:
            self._cluster.check_up(self._client)
            taken: dict[TopicPartition, list[Record]] = {}
            budget = max_records if max_records is not None else 10**9
            for tp in self.assignment:
                log = self._cluster.topics.get(tp.topic, [[]])[tp.partition]
                start = self.position.get(tp, 0)
                chunk = log[start : start + budget]
                if chunk:
                    taken[tp] = chunk
                    self.position[tp] = start + len(chunk)
                    budget -= len(chunk)
                if budget <= 0:
                    break
            if taken or asyncio.get_running_loop().time() >= deadline:
                return taken
            await asyncio.sleep(0.01)

    async def commit(self, offsets: dict[TopicPartition, int] | None = None) -> None:
        self._cluster.check_up(self._client)
        assert self.group_id is not None and offsets is not None
        for tp, offset in offsets.items():
            if tp not in self.assignment:
                raise CommitFailedError()
            self._cluster.groups[self.group_id].committed[tp] = offset


class FakeFactory:
    """What ``KafkaTransport(factory=...)`` takes."""

    def __init__(self, cluster: FakeCluster, client_id: str) -> None:
        self._cluster, self._client = cluster, client_id

    def producer(self) -> FakeProducer:
        return FakeProducer(self._cluster, self._client)

    def consumer(self, group_id: str | None, *, from_start: bool) -> FakeConsumer:
        return FakeConsumer(self._cluster, self._client, group_id, from_start)

    def admin(self) -> FakeAdmin:
        return FakeAdmin(self._cluster, self._client)
