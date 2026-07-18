# Casa

Casa is the Home Assistant companion integration for the Casa add-on. Version
0.5.0 connects one Casa parent to the server's authenticated voice-agent catalog
and creates separate conversation entities for Tina, Gary, and future agents.
Each child keeps a fixed catalog role and its own client, connection,
availability, session, background route, and cleanup lifecycle; a missing or
failing role never falls back to another child.

**Requires:** Home Assistant 2026.4+ and Casa v0.89.0, which exposes the
authenticated `GET /api/voice/agents` schema 1 catalog and coordinated voice
handoff.

Upgrade Casa to v0.89.0 before installing integration v0.5.0. Specialist
handoff uses WebSocket protocol 2 and requires the complete
`background_jobs`, `satellite_announce`, and `voice_handoff` capability set.
Protocol 1 or a missing `voice_handoff` capability fails closed before a job is
created; there is no legacy handoff fallback.

Older servers do not provide the required catalog endpoint and cannot create a
new v0.5.0 entry or reconcile its catalog. An existing v0.5.0 entry with retained children may
load in degraded mode without catalog reconciliation while the endpoint is
unavailable, but it cannot discover new, renamed, or restored agents. The
server release must therefore land before this integration release.

v0.5.0 has no legacy voice-handoff compatibility mode. Upgrade the coordinated
Casa server and integration pair together; protocol-1 routes are not migrated
or used for a fallback specialist job.

The parent Configure action contains only satellite entity overrides. Reconfigure
each agent child to change session mode, WebSocket/SSE transport, or Assist idle
stability; role and name remain catalog-owned. Removing a still-advertised child
causes it to be recreated on the next reload with the same parent-plus-role
identity. Manual duplicates are rejected only for the exact host/port pair, so
different aliases for one server remain a documented limitation.

WebSocket background delivery is enabled only after Casa acknowledges protocol
2 and the complete `background_jobs`, `satellite_announce`, and `voice_handoff`
capability set for that exact agent route. Concierge handoff fails closed before
job creation for protocol 1 or an incomplete registration; there is no legacy
handoff fallback. Its terminal `HandoffFrame` supplies the ordinary Assist
acknowledgement, after which the user may continue or cancel normally; the
completed result is announced when the originating satellite is idle. Butler
home-control turns remain direct. After successful audio, an ordinary WebSocket
reconnect keeps the manager's bounded process-local cache and suppresses a
replay. If its delivered acknowledgement was lost, a repeated summary remains
possible after a manager or integration process restart or delivered-cache
eviction; at-least-once delivery prefers that bounded duplicate risk over a
silently lost result.

Casa checks an HMAC of the empty HTTP upgrade body to authenticate the
integration client when the WebSocket is established. That handshake does not
authenticate individual frames, and plain `ws://` provides no encryption or
cryptographic server authentication. Keep the connection on a trusted
LAN/private network or carry it through an encrypted tunnel.
