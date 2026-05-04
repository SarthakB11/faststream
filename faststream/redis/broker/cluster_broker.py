import logging
import warnings
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from urllib.parse import urlparse

from fast_depends import Provider, dependency_provider
from redis.asyncio.connection import DefaultParser, Encoder

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.redis.broker import RedisBroker
from faststream.redis.broker.broker import _resolve_url_options
from faststream.redis.configs import ConnectionState, RedisBrokerConfig
from faststream.redis.configs.cluster import ClusterConnectionState
from faststream.redis.parser import BinaryMessageFormatV1, MessageFormat
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.redis.subscriber.usecases.basic import LogicSubscriber
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state
from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from redis.asyncio.client import Pipeline
    from redis.asyncio.connection import BaseParser, Connection

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.redis.message import RedisChannelMessage
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.redis.subscriber.usecases import ChannelSubscriber
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


_CLUSTER_INCOMPATIBLE_PARAMS = frozenset({
    "db",
    "socket_read_size",
    "socket_type",
    "retry_on_timeout",
    "parser_class",
    "encoder_class",
    "connection_class",
})


def _clean_cluster_options(
    raw: dict[str, Any],
    *,
    startup_nodes: list[tuple[str, int]] | None = None,
    host: str = EMPTY,
    port: str | int = EMPTY,
) -> dict[str, Any]:
    from redis.asyncio.cluster import ClusterNode

    nodes: list[ClusterNode] = []

    parsed_host = raw.get("host")
    parsed_port = int(raw.get("port", 6379))

    if host is not EMPTY:
        parsed_host = str(host)
    if port is not EMPTY:
        parsed_port = int(port)

    if parsed_host:
        nodes.append(ClusterNode(parsed_host, parsed_port))
    if startup_nodes:
        for h, p in startup_nodes:
            nodes.append(ClusterNode(h, int(p)))

    cleaned = {
        k: v
        for k, v in raw.items()
        if k not in _CLUSTER_INCOMPATIBLE_PARAMS and k not in {"host", "port"}
    }
    cleaned["startup_nodes"] = nodes
    cleaned.pop("connection_class", None)
    return cleaned


class RedisClusterBroker(RedisBroker):
    """Redis Cluster broker.

    Connects to a **Redis Cluster** instead of a single Redis instance.
    Supports List, Stream, and **Sharded Pub/Sub** (``SPUBLISH`` /
    ``SSUBSCRIBE``, requires Redis server >= 7.0).
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        host: str = EMPTY,
        port: str | int = EMPTY,
        db: str | int = EMPTY,
        connection_class: type["Connection"] = EMPTY,
        client_name: str | None = None,
        health_check_interval: float = 0,
        max_connections: int | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_read_size: int = 65536,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        socket_type: int = 0,
        retry_on_timeout: bool = False,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        parser_class: type["BaseParser"] = DefaultParser,
        encoder_class: type["Encoder"] = Encoder,
        graceful_timeout: float | None = 15.0,
        ack_policy: AckPolicy = EMPTY,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[RedisRegistrator] = (),
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
        # --- cluster-specific ---
        startup_nodes: list[tuple[str, int]] | None = None,
    ) -> None:
        self.message_format = message_format

        if specification_url is None:
            specification_url = url
        if protocol is None:
            url_kwargs = urlparse(specification_url)
            protocol = url_kwargs.scheme

        all_options = _resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            db=db,
            client_name=client_name,
            health_check_interval=health_check_interval,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_read_size=socket_read_size,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            socket_type=socket_type,
            retry_on_timeout=retry_on_timeout,
            encoding=encoding,
            encoding_errors=encoding_errors,
            parser_class=parser_class,
            connection_class=connection_class,
            encoder_class=encoder_class,
        )

        cluster_opts = _clean_cluster_options(
            all_options,
            startup_nodes=startup_nodes,
            host=host,
            port=port,
        )

        connection_state = ClusterConnectionState(cluster_opts)

        super(RedisBroker, self).__init__(
            **all_options,
            routers=routers,
            config=RedisBrokerConfig(
                connection=cast("ConnectionState", connection_state),
                producer=RedisFastProducer(
                    connection=cast("ConnectionState", connection_state),
                    parser=parser,
                    decoder=decoder,
                    message_format=self.message_format,
                    serializer=serializer,
                ),
                message_format=self.message_format,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_redis_logger_state(logger=logger, log_level=log_level),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )

    # ── helpers ──────────────────────────────────────────────────────────

    @property
    def _cluster_state(self) -> ClusterConnectionState:
        return cast("ClusterConnectionState", self.config.broker_config.connection)

    # ── Subscriber ─────────────────────────────────────────────────────

    def subscriber(  # type: ignore[override]
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
        **kwargs: Any,
    ) -> "LogicSubscriber":
        if channel is not None:
            return self._make_channel_subscriber(channel, **kwargs)
        return super().subscriber(
            channel=None,
            list=list,
            stream=stream,
            **kwargs,
        )

    def _make_channel_subscriber(
        self,
        channel: Union["PubSub", str],
        **kwargs: Any,
    ) -> "ChannelSubscriber":
        state = self._cluster_state

        sub = cast(
            "ChannelSubscriber",
            super().subscriber(channel=channel, list=None, stream=None, **kwargs),
        )

        async def _patched_start() -> None:
            if sub.subscription:
                return
            psub = state.pubsub()
            sub.subscription = psub  # type: ignore[assignment]
            if sub.channel.pattern:
                await psub.psubscribe(sub.channel.name)
            else:
                await psub.subscribe(sub.channel.name)
            await LogicSubscriber.start(sub, psub)

        sub.start = _patched_start  # type: ignore[method-assign]
        return sub

    # ── Publish ─────────────────────────────────────────────────────────

    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int | bytes:
        if pipeline is not None:
            warnings.warn(
                "Pipeline is not supported in Redis Cluster and will be ignored.",
                category=RuntimeWarning,
                stacklevel=2,
            )
        if channel is not None and list is None and stream is None:
            state = self._cluster_state
            body = self.message_format.encode(
                message=message,
                reply_to="",
                headers={},
                correlation_id="",
            )
            return await state.sync_publish(channel, body)

        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            reply_to=reply_to,
            headers=headers,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )
        return cast(
            "int | bytes",
            await super()._basic_publish(cmd, producer=self.config.producer),
        )

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def _connect(self) -> Any:
        await self.config.connect()
        return self.config.broker_config.connection.client

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    async def start(self) -> None:
        await self.connect()
        await super().start()

    # ── Publish helpers (delegates) ─────────────────────────────────────

    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        channel: str | None = None,
        *,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            headers=headers,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.message_format,
        )
        return cast(
            "RedisChannelMessage",
            await super()._basic_request(cmd, producer=self.config.producer),
        )

    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        if pipeline is not None:
            warnings.warn(
                "Pipeline is not supported in Redis Cluster and will be ignored.",
                category=RuntimeWarning,
                stacklevel=2,
            )
        cmd = RedisPublishCommand(
            *messages,
            list=list,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )
        return cast(
            "int",
            await self._basic_publish_batch(cmd, producer=self.config.producer),
        )
