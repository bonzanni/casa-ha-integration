# Casa

Casa is the Home Assistant companion integration for the Casa add-on. Version
0.5.0 connects one Casa parent to the server's authenticated voice-agent catalog
and creates separate conversation entities for Tina, Gary, and future agents.
Each child keeps a fixed catalog role and its own client, connection,
availability, session, background route, and cleanup lifecycle; a missing or
failing role never falls back to another child.

**Requires:** Home Assistant 2026.4+ and the Casa server release that exposes
authenticated `GET /api/voice/agents` schema 1.

Upgrade the Casa server before installing integration v0.4.0. Older servers do
not provide the required catalog endpoint and cannot create a new v0.4.0 entry
or reconcile its catalog. An existing v0.4.0 entry with retained children may
load in degraded mode without catalog reconciliation while the endpoint is
unavailable, but it cannot discover new, renamed, or restored agents. The
server release must therefore land before this integration release.

v0.4.0 is a clean break from v0.3.0. There is no automatic migration: note the
current pipeline assignments, delete the existing Casa integration entry, add
Casa again, and recreate affected Assist pipelines with the matching discovered
agent.

When Casa runs as a Supervisor app with webhook authentication enabled, Home
Assistant discovers it automatically. Confirm the displayed host and port to
connect; the versioned discovery record supplies the authenticated webhook
secret. If discovery is unavailable, use **Settings → Devices & services → Add
integration → Casa** and enter the exact host, port, and webhook secret
manually. Rediscovery of the same Supervisor UUID updates the stored endpoint
and secret, reloads the parent, and retains its Tina, Gary, and future
voice-agent children.

The parent Configure action contains only satellite entity overrides. Reconfigure
each agent child to change session mode, WebSocket/SSE transport, or Assist idle
stability; role and name remain catalog-owned. Removing a still-advertised child
causes it to be recreated on the next reload with the same parent-plus-role
identity. Manual duplicates are rejected only for the exact host/port pair, so
different aliases for one server remain a documented limitation.

WebSocket background delivery is enabled only after Casa acknowledges protocol
1 and both required capabilities for that exact agent route. After successful
audio, an ordinary WebSocket reconnect keeps the manager's bounded process-local
cache and suppresses a replay. If its delivered acknowledgement was lost, a
repeated summary remains possible after a manager or integration process
restart or delivered-cache eviction; at-least-once delivery prefers that bounded
duplicate risk over a silently lost result.

Casa checks an HMAC of the empty HTTP upgrade body to authenticate the
integration client when the WebSocket is established. That handshake does not
authenticate individual frames, and plain `ws://` provides no encryption or
cryptographic server authentication. Keep the connection on a trusted
LAN/private network or carry it through an encrypted tunnel.
