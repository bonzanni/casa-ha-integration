# Casa HA Integration

Companion custom integration that connects Home Assistant to the
[Casa add-on](https://github.com/bonzanni/casa-ha-app). Integration v0.6.0
creates one **Casa** parent from the server's authenticated voice-agent catalog,
then exposes separate role-stable conversation entities for Butler, Concierge,
and future catalog-discovered agents. Each entity has its own routing, connection,
availability, session settings, and background-delivery lifecycle.

The integration streams TTS deltas, registers voice sessions from
`assist_satellite` state changes, provides a never-silent error path, and
supports acknowledgement-gated delivery of completed background specialist
jobs.

## Requirements

- Home Assistant Core 2026.4 or newer.
- The Casa server release that provides the authenticated
  `GET /api/voice/agents` schema-1 catalog, running and reachable on the local
  network.
- HACS (Home Assistant Community Store).

## Compatibility and coordinated v0.6.0 upgrade

Upgrade Casa to v0.90.0 before installing integration v0.6.0. The releases are
coordinated: specialist handoff uses WebSocket protocol 2 and requires the
complete `background_jobs`, `satellite_announce`, and `voice_handoff`
capability set. Protocol 1 or a missing `voice_handoff` capability fails closed
before a job is created; there is no legacy handoff fallback.

Older Casa servers do not expose `GET /api/voice/agents` and cannot create a new
v0.6.0 entry or reconcile its catalog. An existing v0.6.0 entry with retained children
may load in degraded mode without catalog reconciliation while the endpoint is
unavailable, but it cannot discover new, renamed, or restored agents. This
recovery behavior does not replace the supported server-first upgrade order:
the server endpoint release must land before the integration release.

v0.6.0 has no legacy voice-handoff compatibility mode. Upgrade the coordinated
Casa server and integration pair together; protocol-1 routes are not migrated
or used for a fallback specialist job.

Casa checks an HMAC of the empty HTTP upgrade body to authenticate the
integration client when the WebSocket is established. That handshake does not
authenticate individual frames, and plain `ws://` provides no encryption or
cryptographic server authentication. Keep the Casa-to-Home Assistant
connection on a trusted LAN, private network, or encrypted tunnel.

## Install and configure

1. Upgrade Casa to v0.90.0, which includes the authenticated agent catalog and
   the coordinated protocol-2 voice-handoff contract.
2. In HACS, open **Integrations** → **⋮** → **Custom repositories**.
3. Add `https://github.com/bonzanni/casa-ha-integration` with category
   **Integration**.
4. Install **Casa**, then restart Home Assistant.
5. When Casa is running as a Supervisor app with webhook authentication enabled,
   Home Assistant discovers it automatically. Confirm the displayed host and
   port before connecting; the discovery record carries the authenticated
   webhook secret and creates one parent plus its voice-agent children.
6. If automatic discovery is unavailable, go to **Settings → Devices & services
   → Add integration → Casa** and enter the exact host, port (default `18065`),
   and Casa webhook secret. Manual setup authenticates and validates the same
   complete catalog.
7. In each Assist pipeline, select the matching discovered role: **Casa Butler
   → Voice** for direct home-control turns, **Casa Concierge → Voice** for
   concierge/specialist work, or the relevant future catalog role.

There is no agent role field. The catalog role remains fixed for each discovered
child, so a pipeline cannot silently reroute to a different agent; its mutable
persona alias is descriptive metadata only.

Manual setup rejects another entry with the exact host and port. The Casa
Supervisor UUID remains authoritative when Supervisor discovery is available;
manual setup cannot prove that different aliases for the same host identify the
same installation, so alias-based duplicates remain possible.

Casa publishes a strict, versioned Supervisor discovery record only while
webhook authentication is enabled. On a later discovery for the same Supervisor
UUID, Casa updates the stored endpoint and webhook secret and reloads the
existing parent; its Butler, Concierge, and future voice-agent children are
retained.

### Stable Home Assistant names

Home Assistant uses stable role identity rather than mutable catalog persona
aliases. The integration parent is **Casa**; each discovered role has a device
named **Casa &lt;Role&gt;** and one conversation entity named **Voice**. For example,
Assist shows **Casa Butler → Voice** and **Casa Concierge → Voice**. Casa may
change those persona aliases (Tina, Gary, or other names), but they remain
descriptive device metadata and never become pipeline-service identity.

## Options and agent settings

### Parent configuration

The parent **Configure** action contains only **Satellite entity overrides**.
This optional JSON object maps a Home Assistant device ID to one exact
`assist_satellite` entity when that device exposes multiple candidates. Leave
it as `{}` unless Casa reports an ambiguous satellite; invalid or cross-device
bindings are rejected.

### Per-agent reconfiguration

Use the **Reconfigure** action for the matching voice-agent child. Each child
owns these mutable fields:

- **Session mode** — memory scope derived per `device`, `user`, or
  `conversation`. Butler defaults to device scope, Concierge defaults to
  conversation scope, and future roles default to device scope.
- **Transport** — `ws` (default, with session registration and negotiated
  background delivery) or `sse` (stateless synchronous fallback).
- **Assist idle stability** — how long the satellite must remain idle before a
  queued result plays, from 0–5000 ms (default 750 ms).

Role identity and persona metadata are not editable. Home Assistant cannot make a config
subentry non-removable: if a user deletes a still-advertised voice-agent child,
it is recreated on the next reload with the same parent-plus-role entity and
route identity. If Casa stops advertising a role, its retained entity remains
unavailable and never falls back to another child's client.

## Runtime and background delivery

Every present agent owns a separate API client, WebSocket supervisor, route,
delivery manager, availability state, and cleanup boundary. Only the satellite
directory and state listener are shared. A disconnected, missing, or failing
child does not change another child's availability or routing.

With WebSocket transport, each agent registers a protocol-2 route and
advertises the complete `background_jobs`, `satellite_announce`, and
`voice_handoff` capability set. Concierge handoff is enabled only after Casa
acknowledges every capability on that exact socket generation. Protocol 1 or an
incomplete registration fails closed before Casa creates the specialist job;
there is no legacy handoff fallback. A role using SSE remains available for
synchronous turns without a persistent socket.

For a Concierge specialist request, the terminal `HandoffFrame` is returned as
the normal Assist `ConversationResult`: the user hears the fixed acknowledgement
immediately, may continue or cancel normally, and hears the completed result
when the originating satellite is idle. Butler's direct home-control turns do
not use asynchronous handoff.

Completed work is queued per satellite. If the satellite is already idle it is
announced immediately; otherwise the integration waits for the current voice
interaction to finish and for the configured stable-idle interval. Queues are
FIFO per device, while different devices and different agent routes remain
isolated. A user may cancel before playback starts; playback already underway
is not interrupted.

Casa is the sole durable job owner. The integration records completed job IDs
in a bounded, process-local delivered cache before it sends the acknowledgement.
An ordinary WebSocket reconnect keeps the same manager and cache, so a reoffer
is claimed and acknowledged without repeating the audio. Delivery remains
intentionally at least once: if the announcement succeeds but its delivered
acknowledgement is lost, the concise summary may repeat after a manager or
integration process restart, or after delivered-cache eviction, rather than
being silently lost.

## Release acceptance

Release acceptance has two layers: reproducible real-system exercises and
automated fault-injection gates. Both must pass before publishing v0.6.0.

### Real-system E2E

Run these checks against a real Home Assistant instance and the released Casa
catalog endpoint, with the Casa server upgraded first:

1. Confirm the authenticated catalog accepts the configured webhook secret and
   rejects a bad secret.
2. Add one Casa parent and verify it creates **Casa Butler → Voice** and
   **Casa Concierge → Voice**, with separate stable entity IDs. Confirm no role selector appears
   on the parent or agent forms.
3. Select Casa Butler → Voice in a pipeline. After one warm-up turn, measure at least 20 direct
   turns with monotonic timestamps and require server
   utterance-to-first-text-block p95 below 1.5 seconds and Home Assistant
   end-of-speech-to-first-audible-output p95 below 3.0 seconds. Confirm every
   request carries `butler` and the configured Butler session scope.
4. Select Casa Concierge → Voice in another pipeline. Complete a Concierge background result while
   the satellite is busy; verify it is announced only after stable idle and
   uses Concierge's route, not Butler's.
5. Barge in during Butler TTS. Verify Casa receives a `cancel` frame for that
   utterance, Home Assistant recovers, and the next turn succeeds.
6. Replace the Casa webhook secret. Confirm terminal authentication starts one
   parent reauthentication flow, and entering the new secret restores every
   child without duplicate reauth prompts.

### Controlled fault-injection acceptance

These are automated end-to-end gates, not ad hoc live checks. Run them with a
controllable Casa catalog fixture, a WebSocket protocol proxy, and Home
Assistant task and socket introspection so the injected boundary and every
required observable are deterministic:

1. Make the catalog fixture return malformed schema and malformed agent data;
   verify validation is atomic and the existing children do not partially
   change.
2. Stop advertising one role, reload, and verify the missing role remains
   unavailable while the other entity continues to connect and answer. Restore
   the role and confirm the same stable identity returns.
3. Move a satellite from a non-listening state into `LISTENING` and verify each
   connected WebSocket child registers the device once for that wake edge. An
   attribute-only repeated `LISTENING` state update must not send another
   registration. Switch one child to SSE and confirm it remains synchronously
   usable without session registration or a background socket.
4. Disconnect one Casa route mid-conversation through the protocol proxy. That
   entity must speak the fallback line and reconnect without an integration
   reload; the sibling remains usable and receives no availability transition.
5. Exercise isolated cleanup: unload or reload with both agents active and use
   task and socket introspection to verify every child socket, delivery worker,
   registration task, and the one shared listener close without leaking or
   rerouting work between children.
6. Have the protocol proxy drop Concierge's WebSocket after playback but before the
   delivered acknowledgement. An ordinary reconnect must claim and acknowledge
   the reoffer without replay; after an integration restart the summary may
   replay once but must never be silently lost.

## Architecture and limitations

The parent owns host, port, webhook secret, catalog health, satellite override,
and the shared satellite directory/listener. Catalog-owned child entries own a
fixed role/name plus mutable session, transport, and idle settings. Reload
reconciliation adds and renames children through public Home Assistant APIs but
does not delete a role merely because it is temporarily absent.

- Manual duplicate prevention compares the exact host/port pair; aliases for
  the same server remain the documented limitation described above.
- Voice-ID speaker promotion depends on Home Assistant exposing a stable
  `user_input.context.user_id`.
- Home Assistant Assist output does not support SSML; Casa sends plain text.
- Unit tests use lightweight Home Assistant stubs rather than the full
  pytest-Home-Assistant harness, so both release-acceptance layers remain gates
  alongside tests, hassfest, and HACS validation.

## License

MIT.
