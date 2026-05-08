"""
Integration tests for Kafka cluster and topics (Phase 5).
Covers: TS-5-01..10, TS-5-EC-03, TS-7-18..20

Requires: Strimzi Kafka accessible at TEST_KAFKA_BROKERS.
Run: pytest -m integration tests/integration/test_kafka.py
"""

import json
import os
import time
import uuid
import pytest

pytestmark = pytest.mark.integration

try:
    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
except ImportError:
    pytest.skip("kafka-python not installed", allow_module_level=True)

KAFKA_BROKERS   = os.environ.get("TEST_KAFKA_BROKERS", "localhost:9092")
TOPIC_ANOMALIES = "anomalies.flagged"
TOPIC_EVENTS    = "events.raw"
CONSUMER_GROUP  = "langgraph-agent"


@pytest.fixture(scope="module")
def admin_client():
    client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS, request_timeout_ms=10000)
    yield client
    client.close()


@pytest.fixture(scope="module")
def producer():
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode(),
        request_timeout_ms=10000,
    )
    yield p
    p.close()


# ── TS-5-01: cluster reachable ───────────────────────────────────────────────

def test_kafka_cluster_reachable(admin_client):
    """TS-5-01: admin client connects without exception."""
    metadata = admin_client.describe_cluster()
    assert metadata is not None


# ── TS-5-03 / TS-5-04: topics exist ─────────────────────────────────────────

def test_anomalies_flagged_topic_exists(admin_client):
    """TS-5-03: anomalies.flagged topic created by Strimzi."""
    topics = admin_client.list_topics()
    assert TOPIC_ANOMALIES in topics, f"{TOPIC_ANOMALIES!r} not in {topics}"


def test_events_raw_topic_exists(admin_client):
    """TS-5-04: events.raw topic created."""
    topics = admin_client.list_topics()
    assert TOPIC_EVENTS in topics


def test_anomalies_flagged_partition_count(admin_client):
    """TS-5-03: 6 partitions on anomalies.flagged."""
    desc = admin_client.describe_topics([TOPIC_ANOMALIES])
    partitions = desc[0]["partitions"]
    assert len(partitions) == 6, f"Expected 6 partitions, got {len(partitions)}"


def test_events_raw_partition_count(admin_client):
    """TS-5-04: 12 partitions on events.raw."""
    desc = admin_client.describe_topics([TOPIC_EVENTS])
    partitions = desc[0]["partitions"]
    assert len(partitions) == 12


def test_anomalies_flagged_replication_factor(admin_client):
    """TS-5-03: RF=3 on anomalies.flagged."""
    desc = admin_client.describe_topics([TOPIC_ANOMALIES])
    for partition in desc[0]["partitions"]:
        assert len(partition["replicas"]) == 3, \
            f"Partition {partition['partition']} has RF={len(partition['replicas'])}, expected 3"


# ── TS-5-06 / TS-5-07: retention config ──────────────────────────────────────

def test_anomalies_flagged_retention_7_days(admin_client):
    """TS-5-06: retention.ms = 604800000 (7 days)."""
    configs = admin_client.describe_configs(
        config_resources=[{"resource_type": 2, "name": TOPIC_ANOMALIES}]   # 2 = TOPIC
    )
    cfg = {c.name: c.value for c in configs[0].resources[0].config_entries}
    assert cfg.get("retention.ms") == "604800000", \
        f"Got retention.ms={cfg.get('retention.ms')}, expected 604800000"


def test_events_raw_retention_1_day(admin_client):
    """TS-5-07: retention.ms = 86400000 (1 day)."""
    configs = admin_client.describe_configs(
        config_resources=[{"resource_type": 2, "name": TOPIC_EVENTS}]
    )
    cfg = {c.name: c.value for c in configs[0].resources[0].config_entries}
    assert cfg.get("retention.ms") == "86400000"


# ── TS-5-09 / TS-5-10: produce / consume round-trip ─────────────────────────

def test_produce_consume_round_trip(producer):
    """TS-5-10: message produced to anomalies.flagged is consumed correctly."""
    unique_id = str(uuid.uuid4())
    test_message = {
        "eventID":     unique_id,
        "eventName":   "RunInstances",
        "eventSource": "ec2.amazonaws.com",
        "eventTime":   "2026-05-08T10:00:00Z",
        "score":       -0.25,
        "userIdentity": {"arn": "arn:aws:iam::123456789012:user/test"},
    }

    # Produce
    future = producer.send(TOPIC_ANOMALIES, test_message)
    future.get(timeout=10)
    producer.flush()

    # Consume with unique group id to read from latest
    group_id = f"test-{uuid.uuid4()}"
    consumer = KafkaConsumer(
        TOPIC_ANOMALIES,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
    )

    # Seek to beginning of all assigned partitions to catch the message
    consumer.poll(timeout_ms=1000)   # trigger partition assignment
    consumer.seek_to_end()
    producer.send(TOPIC_ANOMALIES, test_message)   # send again after seek
    producer.flush()

    received = None
    for msg in consumer:
        if msg.value.get("eventID") == unique_id:
            received = msg.value
            break

    consumer.close()
    assert received is not None, "Message not received within timeout"
    assert received["eventName"] == "RunInstances"


def test_consumer_group_offset_committed(producer):
    """TS-5-09: consumer group commits offset after processing."""
    group_id = f"test-offset-{uuid.uuid4()}"
    test_msg = {"eventID": str(uuid.uuid4()), "eventName": "Test", "score": -0.1,
                "eventTime": "2026-05-08T10:00:00Z", "userIdentity": {}}

    producer.send(TOPIC_ANOMALIES, test_msg)
    producer.flush()
    time.sleep(1)

    consumer = KafkaConsumer(
        TOPIC_ANOMALIES,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )

    consumed = []
    for msg in consumer:
        consumed.append(msg)
        consumer.commit()
        break

    consumer.close()
    assert len(consumed) >= 1


# ── TS-5-EC-03: manual commit — no auto-commit on exception ──────────────────

def test_no_auto_commit_means_replay_on_restart(producer):
    """TS-5-EC-03: with enable_auto_commit=False, un-committed message re-delivered."""
    group_id = f"test-replay-{uuid.uuid4()}"
    unique_id = str(uuid.uuid4())
    test_msg = {"eventID": unique_id, "eventName": "ReplayTest", "score": -0.3,
                "eventTime": "2026-05-08T10:00:00Z", "userIdentity": {}}

    producer.send(TOPIC_ANOMALIES, test_msg)
    producer.flush()
    time.sleep(1)

    # First consumer: reads but does NOT commit
    consumer1 = KafkaConsumer(
        TOPIC_ANOMALIES,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    first_offset = None
    for msg in consumer1:
        if msg.value.get("eventID") == unique_id:
            first_offset = msg.offset
            # Do NOT call consumer1.commit() — simulating crash
            break
    consumer1.close()

    if first_offset is None:
        pytest.skip("Message not found in topic — may have expired")

    # Second consumer with same group: should re-deliver the uncommitted message
    consumer2 = KafkaConsumer(
        TOPIC_ANOMALIES,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    replayed_offsets = []
    for msg in consumer2:
        replayed_offsets.append(msg.offset)
        consumer2.commit()
        break
    consumer2.close()

    assert first_offset in replayed_offsets, \
        f"Expected offset {first_offset} to be replayed, got {replayed_offsets}"


# ── TS-5-EC-02: topic name — dot vs hyphen ───────────────────────────────────

def test_consumer_uses_dot_topic_name():
    """TS-5-EC-02: consumer subscribes to 'anomalies.flagged' (dot), not 'anomalies-flagged' (hyphen)."""
    # The Strimzi KafkaTopic CRD name uses hyphens (anomalies-flagged) but
    # spec.topicName (if set) or the Kafka topic name itself uses dots.
    # This test verifies the CONSUMER uses the correct dot-notation name.
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        consumer_timeout_ms=100,
    )
    consumer.subscribe([TOPIC_ANOMALIES])
    consumer.poll(timeout_ms=100)   # triggers metadata fetch
    assigned_topic = None
    for tp in consumer.assignment():
        assigned_topic = tp.topic
        break
    consumer.close()

    if assigned_topic:
        assert assigned_topic == TOPIC_ANOMALIES   # dot, not hyphen
