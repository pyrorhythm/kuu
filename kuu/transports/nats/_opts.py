import ssl
from typing import NotRequired, TypedDict

import nats.aio.client as nc

_defaults = {
	"connect_timeout": nc.DEFAULT_CONNECT_TIMEOUT,
	"reconnect_time_wait": nc.DEFAULT_RECONNECT_TIME_WAIT,
	"max_reconnect_attempts": nc.DEFAULT_MAX_RECONNECT_ATTEMPTS,
	"ping_interval": nc.DEFAULT_PING_INTERVAL,
	"max_outstanding_pings": nc.DEFAULT_MAX_OUTSTANDING_PINGS,
	"flusher_queue_size": nc.DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
	"drain_timeout": nc.DEFAULT_DRAIN_TIMEOUT,
	"inbox_prefix": nc.DEFAULT_INBOX_PREFIX,
	"pending_size": nc.DEFAULT_PENDING_SIZE,
}


class NatsConnectOptions(TypedDict):
	error_cb: NotRequired[nc.ErrorCallback | None]
	disconnected_cb: NotRequired[nc.Callback | None]
	closed_cb: NotRequired[nc.Callback | None]
	discovered_server_cb: NotRequired[nc.Callback | None]
	reconnected_cb: NotRequired[nc.Callback | None]
	name: NotRequired[str | None]
	pedantic: NotRequired[bool]
	verbose: NotRequired[bool]
	allow_reconnect: NotRequired[bool]
	connect_timeout: NotRequired[int]
	reconnect_time_wait: NotRequired[int]
	max_reconnect_attempts: NotRequired[int]
	ping_interval: NotRequired[int]
	max_outstanding_pings: NotRequired[int]
	dont_randomize: NotRequired[bool]
	flusher_queue_size: NotRequired[int]
	no_echo: NotRequired[bool]
	tls: NotRequired[ssl.SSLContext | None]
	tls_hostname: NotRequired[str | None]
	tls_handshake_first: NotRequired[bool]
	user: NotRequired[str | None]
	password: NotRequired[str | None]
	token: NotRequired[str | nc.TokenCallback | None]
	drain_timeout: NotRequired[int]
	signature_cb: NotRequired[nc.SignatureCallback | None]
	user_jwt_cb: NotRequired[nc.JWTCallback | None]
	user_credentials: NotRequired[nc.Credentials | None]
	nkeys_seed: NotRequired[str | None]
	nkeys_seed_str: NotRequired[str | None]
	inbox_prefix: NotRequired[str | bytes]
	pending_size: NotRequired[int]
	flush_timeout: NotRequired[float | None]
	ws_connection_headers: NotRequired[dict[str, list[str]] | None]
	reconnect_to_server_handler: NotRequired[nc.ReconnectToServerHandler | None]
