# coding: utf-8
import base64
from collections import defaultdict
from functools import partial
import gzip
import os
import struct
import threading
import time
import typing
from typing import DefaultDict  # noqa:F401
from typing import Dict  # noqa:F401
from typing import List  # noqa:F401
from typing import NamedTuple  # noqa:F401
from typing import Optional  # noqa:F401
from typing import Union  # noqa:F401

import ddtrace
from ddtrace import config
from ddtrace.internal import compat
from ddtrace.internal.atexit import register_on_exit_signal
from ddtrace.internal.constants import DEFAULT_SERVICE_NAME
from ddtrace.internal.core import DDSketch
from ddtrace.internal.utils.retry import fibonacci_backoff_with_jitter

from .._encoding import packb
from ..agent import get_connection
from ..compat import get_connection_response
from ..forksafe import Lock
from ..hostname import get_hostname
from ..logger import get_logger
from ..periodic import PeriodicService
from ..writer import _human_size
from .encoding import decode_var_int_64
from .encoding import encode_var_int_64
from .fnv import fnv1_64
from .schemas.schema_builder import SchemaBuilder
from .schemas.schema_sampler import SchemaSampler


def gzip_compress(payload):
    return gzip.compress(payload, 1)


"""
The data streams processor aggregate stats about pathways (linked chains of services and topics)
And example of a pathway would be:

service 1 --> Kafka topic A --> service 2 --> kafka topic B --> service 3

The processor flushes stats periodically (every 10 sec) to the Datadog agent.
This powers the data streams monitoring product. More details about the product can be found here:
https://docs.datadoghq.com/data_streams/
"""


log = get_logger(__name__)

PROPAGATION_KEY = "dd-pathway-ctx"
PROPAGATION_KEY_BASE_64 = "dd-pathway-ctx-base64"
SHUTDOWN_TIMEOUT = 5

"""
PathwayAggrKey uniquely identifies a pathway to aggregate stats on.
"""
PathwayAggrKey = typing.Tuple[
    str,  # edge tags
    int,  # hash_value
    int,  # parent hash
]


class SumCount:
    """Helper class to keep track of sum and count of values."""

    __slots__ = ("_sum", "_count")

    def __init__(self) -> None:
        self._sum: float = 0.0
        self._count: int = 0

    def add(self, value: float) -> None:
        self._sum += value
        self._count += 1

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def count(self) -> int:
        return self._count


class PathwayStats(object):
    """Aggregated pathway statistics."""

    __slots__ = ("full_pathway_latency", "edge_latency", "payload_size")

    def __init__(self):
        self.full_pathway_latency = DDSketch()
        self.edge_latency = DDSketch()
        self.payload_size = SumCount()


PartitionKey = NamedTuple("PartitionKey", [("topic", str), ("partition", int)])
ConsumerPartitionKey = NamedTuple("ConsumerPartitionKey", [("group", str), ("topic", str), ("partition", int)])
Bucket = NamedTuple(
    "Bucket",
    [
        ("pathway_stats", DefaultDict[PathwayAggrKey, PathwayStats]),
        ("latest_produce_offsets", DefaultDict[PartitionKey, int]),
        ("latest_commit_offsets", DefaultDict[ConsumerPartitionKey, int]),
    ],
)


class DataStreamsProcessor(PeriodicService):
    """DataStreamsProcessor for computing, collecting and submitting data stream stats to the Datadog Agent."""

    def __init__(self, agent_url, interval=None, timeout=1.0, retry_attempts=3):
        # type: (str, Optional[float], float, int) -> None
        if interval is None:
            interval = float(os.getenv("_DD_TRACE_STATS_WRITER_INTERVAL") or 10.0)  # noqa: DDC001
        super(DataStreamsProcessor, self).__init__(interval=interval)
        self._agent_url = agent_url
        self._endpoint = "/v0.1/pipeline_stats"
        self._agent_endpoint = "%s%s" % (self._agent_url, self._endpoint)
        self._timeout = timeout
        # Have the bucket size match the interval in which flushes occur.
        self._bucket_size_ns = int(interval * 1e9)  # type: int
        self._buckets = defaultdict(
            lambda: Bucket(defaultdict(PathwayStats), defaultdict(int), defaultdict(int))
        )  # type: DefaultDict[int, Bucket]
        self._headers = {
            "Datadog-Meta-Lang": "python",
            "Datadog-Meta-Tracer-Version": ddtrace.__version__,
            "Content-Type": "application/msgpack",
            "Content-Encoding": "gzip",
        }  # type: Dict[str, str]
        self._hostname = compat.ensure_text(get_hostname())
        self._service = compat.ensure_text(config._get_service(DEFAULT_SERVICE_NAME))
        self._lock = Lock()
        self._current_context = threading.local()
        self._enabled = True
        self._schema_samplers: Dict[str, SchemaSampler] = {}

        self._flush_stats_with_backoff = fibonacci_backoff_with_jitter(
            attempts=retry_attempts,
            initial_wait=0.618 * self.interval / (1.618**retry_attempts) / 2,
        )(self._flush_stats)

        register_on_exit_signal(partial(_atexit, obj=self))
        self.start()

    def on_checkpoint_creation(
        self, hash_value, parent_hash, edge_tags, now_sec, edge_latency_sec, full_pathway_latency_sec, payload_size=0
    ):
        # type: (int, int, List[str], float, float, float, int) -> None
        """
        on_checkpoint_creation is called every time a new checkpoint is created on a pathway. It records the
        latency to the previous checkpoint in the pathway (edge latency),
        and the latency from the very first element in the pathway (full_pathway_latency)
        the pathway is hashed to reduce amount of information transmitted in headers.

        :param hash_value: hash of the pathway, it's a hash of the edge leading to this point, and the parent hash.
        :param parent_hash: hash of the previous step in the pathway
        :param edge_tags: all tags associated with the edge leading to this step in the pathway
        :param now_sec: current time
        :param edge_latency_sec: latency of the direct edge between the previous point
            in the pathway, and the current step
        :param full_pathway_latency_sec: latency from the very start of the pathway.
        :return: Nothing
        """
        if not self._enabled:
            return

        now_ns = int(now_sec * 1e9)

        with self._lock:
            # Align the span into the corresponding stats bucket
            bucket_time_ns = now_ns - (now_ns % self._bucket_size_ns)
            aggr_key = (",".join(edge_tags), hash_value, parent_hash)
            stats = self._buckets[bucket_time_ns].pathway_stats[aggr_key]
            stats.full_pathway_latency.add(full_pathway_latency_sec)
            stats.edge_latency.add(edge_latency_sec)
            stats.payload_size.add(payload_size)
            self._buckets[bucket_time_ns].pathway_stats[aggr_key] = stats

    def track_kafka_produce(self, topic, partition, offset, now_sec):
        now_ns = int(now_sec * 1e9)
        key = PartitionKey(topic, partition)
        with self._lock:
            bucket_time_ns = now_ns - (now_ns % self._bucket_size_ns)
            self._buckets[bucket_time_ns].latest_produce_offsets[key] = max(
                offset, self._buckets[bucket_time_ns].latest_produce_offsets[key]
            )

    def track_kafka_commit(self, group, topic, partition, offset, now_sec):
        now_ns = int(now_sec * 1e9)
        key = ConsumerPartitionKey(group, topic, partition)
        with self._lock:
            bucket_time_ns = now_ns - (now_ns % self._bucket_size_ns)
            self._buckets[bucket_time_ns].latest_commit_offsets[key] = max(
                offset, self._buckets[bucket_time_ns].latest_commit_offsets[key]
            )

    def _serialize_buckets(self):
        # type: () -> List[Dict]
        """Serialize and update the buckets."""
        serialized_buckets = []
        serialized_bucket_keys = []
        for bucket_time_ns, bucket in self._buckets.items():
            bucket_aggr_stats = []
            backlogs = []
            serialized_bucket_keys.append(bucket_time_ns)

            for aggr_key, stat_aggr in bucket.pathway_stats.items():
                edge_tags, hash_value, parent_hash = aggr_key
                serialized_bucket = {
                    "EdgeTags": [compat.ensure_text(tag) for tag in edge_tags.split(",")],
                    "Hash": hash_value,
                    "ParentHash": parent_hash,
                    "PathwayLatency": stat_aggr.full_pathway_latency.to_proto(),
                    "EdgeLatency": stat_aggr.edge_latency.to_proto(),
                }
                bucket_aggr_stats.append(serialized_bucket)
            for consumer_key, offset in bucket.latest_commit_offsets.items():
                backlogs.append(
                    {
                        "Tags": [
                            "type:kafka_commit",
                            "consumer_group:" + consumer_key.group,
                            "topic:" + consumer_key.topic,
                            "partition:" + str(consumer_key.partition),
                        ],
                        "Value": offset,
                    }
                )
            for producer_key, offset in bucket.latest_produce_offsets.items():
                backlogs.append(
                    {
                        "Tags": [
                            "type:kafka_produce",
                            "topic:" + producer_key.topic,
                            "partition:" + str(producer_key.partition),
                        ],
                        "Value": offset,
                    }
                )
            serialized_buckets.append(
                {
                    "Start": bucket_time_ns,
                    "Duration": self._bucket_size_ns,
                    "Stats": bucket_aggr_stats,
                    "Backlogs": backlogs,
                }
            )

        # Clear out buckets that have been serialized
        for key in serialized_bucket_keys:
            del self._buckets[key]

        return serialized_buckets

    def _flush_stats(self, payload):
        # type: (bytes) -> None
        try:
            conn = get_connection(self._agent_url, self._timeout)
            conn.request("POST", self._endpoint, payload, self._headers)
            resp = get_connection_response(conn)
        except Exception:
            log.debug("failed to submit pathway stats to the Datadog agent at %s", self._agent_endpoint, exc_info=True)
            raise
        else:
            if resp.status == 404:
                log.error("Datadog agent does not support data streams monitoring. Upgrade to 7.34+")
                return
            elif resp.status >= 400:
                log.error(
                    "failed to send data stream stats payload, %s (%s) (%s) response from Datadog agent at %s",
                    resp.status,
                    resp.reason,
                    resp.read(),
                    self._agent_endpoint,
                )
            else:
                log.debug("sent %s to %s", _human_size(len(payload)), self._agent_endpoint)

    def periodic(self):
        # type: () -> None

        with self._lock:
            serialized_stats = self._serialize_buckets()

        if not serialized_stats:
            log.debug("No data streams reported. Skipping flushing.")
            return
        raw_payload = {
            "Service": self._service,
            "TracerVersion": ddtrace.__version__,
            "Lang": "python",
            "Stats": serialized_stats,
            "Hostname": self._hostname,
        }  # type: Dict[str, Union[List[Dict], str]]
        if config.env:
            raw_payload["Env"] = compat.ensure_text(config.env)
        if config.version:
            raw_payload["Version"] = compat.ensure_text(config.version)

        payload = packb(raw_payload)
        compressed = gzip_compress(payload)
        try:
            self._flush_stats_with_backoff(compressed)
        except Exception:
            log.error(
                "retry limit exceeded submitting pathway stats to the Datadog agent at %s",
                self._agent_endpoint,
                exc_info=True,
            )

    def shutdown(self, timeout):
        # type: (Optional[float]) -> None
        self.periodic()
        self.stop(timeout)

    def decode_pathway(self, data):
        # type: (bytes) -> DataStreamsCtx
        try:
            hash_value = struct.unpack("<Q", data[:8])[0]
            data = data[8:]
            pathway_start_ms, data = decode_var_int_64(data)
            current_edge_start_ms, data = decode_var_int_64(data)
            ctx = DataStreamsCtx(self, hash_value, float(pathway_start_ms) / 1e3, float(current_edge_start_ms) / 1e3)
            # reset context of current thread every time we decode
            self._current_context.value = ctx
            return ctx
        except (EOFError, TypeError):
            return self.new_pathway()

    def decode_pathway_b64(self, data):
        # type: (Optional[Union[str, bytes]]) -> DataStreamsCtx
        if not data:
            return self.new_pathway()

        if isinstance(data, str):
            binary_pathway = data.encode("utf-8")
        else:
            binary_pathway = data

        encoded_pathway = base64.b64decode(binary_pathway)
        data_streams_context = self.decode_pathway(encoded_pathway)
        return data_streams_context

    def new_pathway(self, now_sec=None):
        """
        type: (Optional[int]) -> DataStreamsCtx
        :param now_sec: optional start time of this path. Use for services like Kinesis which
                           we aren't getting path information for.
        """

        if not now_sec:
            now_sec = time.time()
        ctx = DataStreamsCtx(self, 0, now_sec, now_sec)
        return ctx

    def set_checkpoint(self, tags, now_sec=None, payload_size=0, span=None):
        """
        type: (List[str], Optional[int], Optional[int]) -> DataStreamsCtx
        :param tags: a list of strings identifying the pathway and direction
        :param now_sec: The time in seconds to count as "now" when computing latencies
        :param payload_size: The size of the payload being sent in bytes
        """

        if not now_sec:
            now_sec = time.time()
        if hasattr(self._current_context, "value"):
            ctx = self._current_context.value
        else:
            ctx = self.new_pathway()
            self._current_context.value = ctx
        if "direction:out" in tags:
            # Add the header for this now, as the callee doesn't have access
            # when producing
            payload_size += len(ctx.encode_b64()) + len(PROPAGATION_KEY_BASE_64)
        ctx.set_checkpoint(tags, now_sec=now_sec, payload_size=payload_size, span=span)
        return ctx

    def try_sample_schema(self, topic):
        now_ms = time.time() * 1000

        sampler = self._schema_samplers.setdefault(topic, SchemaSampler())
        return sampler.try_sample(now_ms)

    def can_sample_schema(self, topic):
        now_ms = time.time() * 1000

        sampler = self._schema_samplers.setdefault(topic, SchemaSampler())
        return sampler.can_sample(now_ms)

    def get_schema(self, schema_name, iterator):
        return SchemaBuilder.get_schema(schema_name, iterator)


class DataStreamsCtx:
    def __init__(self, processor, hash_value, pathway_start_sec, current_edge_start_sec):
        # type: (DataStreamsProcessor, int, float, float) -> None
        self.processor = processor
        self.pathway_start_sec = pathway_start_sec
        self.current_edge_start_sec = current_edge_start_sec
        self.hash = hash_value
        self.service = compat.ensure_text(config._get_service(DEFAULT_SERVICE_NAME))
        self.env = compat.ensure_text(config.env or "none")
        # loop detection logic
        self.previous_direction = ""
        self.closest_opposite_direction_hash = 0
        self.closest_opposite_direction_edge_start = current_edge_start_sec

    def encode(self):
        # type: () -> bytes
        return (
            struct.pack("<Q", self.hash)
            + encode_var_int_64(int(self.pathway_start_sec * 1e3))
            + encode_var_int_64(int(self.current_edge_start_sec * 1e3))
        )

    def encode_b64(self):
        # type: () -> str
        encoded_pathway = self.encode()
        binary_pathway = base64.b64encode(encoded_pathway)
        data_streams_context = binary_pathway.decode("utf-8")
        return data_streams_context

    def _compute_hash(self, tags, parent_hash):
        def get_bytes(s):
            return bytes(s, encoding="utf-8")

        b = get_bytes(self.service) + get_bytes(self.env)
        for t in tags:
            b += get_bytes(t)
        node_hash = fnv1_64(b)
        return fnv1_64(struct.pack("<Q", node_hash) + struct.pack("<Q", parent_hash))

    def set_checkpoint(
        self,
        tags,
        now_sec=None,
        edge_start_sec_override=None,
        pathway_start_sec_override=None,
        payload_size=0,
        span=None,
    ):
        """
        type: (List[str], float, float, float) -> None

        :param tags: an list of tags identifying the pathway and direction
        :param now_sec: The time in seconds to count as "now" when computing latencies
        :param edge_start_sec_override: Use this to override the starting time of an edge
        :param pathway_start_sec_override: Use this to override the starting time of a pathway
        """
        if not now_sec:
            now_sec = time.time()
        tags = sorted(tags)
        direction = ""
        for t in tags:
            if t.startswith("direction:"):
                direction = t
                break
        if direction == self.previous_direction:
            self.hash = self.closest_opposite_direction_hash
            if self.hash == 0:
                # if the closest hash from opposite direction is 0, that means we produce in a loop, without consuming
                # in that case, we don't want the pathway to be longer and longer, but we want to restart a new pathway.
                self.current_edge_start_sec = now_sec
                self.pathway_start_sec = now_sec
            else:
                self.current_edge_start_sec = self.closest_opposite_direction_edge_start
        else:
            self.previous_direction = direction
            self.closest_opposite_direction_hash = self.hash
            self.closest_opposite_direction_edge_start = now_sec

        if edge_start_sec_override:
            self.current_edge_start_sec = edge_start_sec_override

        if pathway_start_sec_override:
            self.pathway_start_sec = pathway_start_sec_override

        parent_hash = self.hash
        hash_value = self._compute_hash(tags, parent_hash)
        if span:
            span.set_tag_str("pathway.hash", str(hash_value))
        edge_latency_sec = max(now_sec - self.current_edge_start_sec, 0.0)
        pathway_latency_sec = max(now_sec - self.pathway_start_sec, 0.0)
        self.hash = hash_value
        self.current_edge_start_sec = now_sec
        self.processor.on_checkpoint_creation(
            hash_value, parent_hash, tags, now_sec, edge_latency_sec, pathway_latency_sec, payload_size=payload_size
        )


class DsmPathwayCodec:
    """
    DsmPathwayCodec is responsible for:
        - encoding and injecting DSM pathway context into produced message headers
        - extracting and decoding DSM pathway context from consumed message headers
    """

    @staticmethod
    def encode(ctx, carrier):
        # type: (DataStreamsCtx, dict) -> None
        if not isinstance(ctx, DataStreamsCtx) or not ctx or not ctx.hash:
            return
        carrier[PROPAGATION_KEY_BASE_64] = ctx.encode_b64()

    @staticmethod
    def decode(carrier, data_streams_processor):
        # type: (dict, DataStreamsProcessor) -> DataStreamsCtx
        if not carrier:
            return data_streams_processor.new_pathway()

        ctx = None
        if PROPAGATION_KEY_BASE_64 in carrier:
            # decode V2 base64 encoding
            ctx = data_streams_processor.decode_pathway_b64(carrier[PROPAGATION_KEY_BASE_64])
        elif PROPAGATION_KEY in carrier:
            # decode V1 encoding
            ctx = data_streams_processor.decode_pathway(carrier[PROPAGATION_KEY])

            if ctx.hash == 0:
                try:
                    # cover case where base64 encoding was included under depcreated key
                    ctx = data_streams_processor.decode_pathway_b64(carrier[PROPAGATION_KEY])
                except Exception:
                    ctx = None
        if not ctx:
            return data_streams_processor.new_pathway()
        return ctx


def _atexit(obj=None):
    try:
        # Data streams tries to flush data on shutdown.
        # Adding a try except here to ensure we don't crash the application if the agent is killed before
        # the application for example.
        obj.shutdown(SHUTDOWN_TIMEOUT)
    except Exception as e:
        if config._data_streams_enabled:
            log.warning("Failed to shutdown data streams processor: %s", repr(e))
