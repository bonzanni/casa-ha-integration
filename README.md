# Casa HA Integration

Companion custom integration that routes Home Assistant's Assist pipeline through the [Casa add-on](https://github.com/bonzanni/casa-ha-app) — a multi-agent AI assistant built around Claude. It ships streaming TTS deltas, voice session registration via `assist_satellite` state listening, a "never silent" error path, and acknowledgement-gated delivery of completed background specialist jobs.

## Requirements

- Home Assistant Core 2026.4 or newer.
- Casa add-on running and reachable on the local network.
- HACS (Home Assistant Community Store) installed.

Casa checks an HMAC of the empty HTTP upgrade body to authenticate the
integration client when the WebSocket is established. That handshake does not
authenticate individual frames, and plain `ws://` provides no encryption or
cryptographic server authentication. Keep the Casa-to-Home Assistant
connection on a trusted LAN, private network, or encrypted tunnel.

## Install

1. In HACS, open **Integrations** → **⋮** → **Custom repositories**.
2. Add `https://github.com/bonzanni/casa-ha-integration` with category **Integration**.
3. Install **Casa** from the list, then restart Home Assistant.
4. Go to **Settings → Devices & services → Add integration → Casa**.
5. Enter host (e.g. `casa-addon`), port (default `18065`), and the webhook secret configured on your Casa add-on.
6. Assign **Casa Butler** as the conversation agent of any Assist pipeline you want Casa to handle.

## Options

Open the integration **Configure** panel to change:

- **Agent role** — which Casa agent handles turns. Default: `butler`.
- **Assist idle stability** — how long an Assist satellite must remain idle
  before queued background audio starts (0–5000 ms; default: 750 ms). A
  satellite that was already stably idle is not delayed.
- **Satellite entity overrides** — optional JSON device-to-entity bindings for
  devices that expose more than one `assist_satellite` entity.
- **Session mode** — how memory scope is derived: `device` (default), `user`, `conversation`.
- **Transport** — `ws` (default, enables barge-in, registration, and negotiated
  background delivery) or `sse` (stateless synchronous fallback).

## Background specialist delivery

With WebSocket transport, the integration eagerly registers a protocol-1 route
and advertises `background_jobs` plus `satellite_announce`. Background delivery
is enabled only after Casa acknowledges both capabilities on that exact socket
generation. Old Casa releases do not acknowledge the registration, so the
integration remains `background_capable=false` while ordinary synchronous Tina
and Gary turns continue unchanged.

Completed work is queued per satellite. If the satellite is already idle it is
announced immediately; otherwise the integration waits for the current voice
interaction to finish and for the configured stable-idle interval. Queues are
FIFO per device, while different devices can progress independently. A user may
cancel before playback starts; playback already underway is not interrupted.

Casa is the sole durable job owner. The integration records completed job IDs
in a bounded, process-local delivered cache before it sends the acknowledgement.
An ordinary WebSocket reconnect keeps the same manager and cache, so a reoffer
is claimed and acknowledged without repeating the audio. Delivery remains
intentionally at least once: if the announcement succeeds but its delivered
acknowledgement is lost, the concise summary may repeat after a manager or
integration process restart, or after delivered-cache eviction, rather than
being silently lost.

## E2E checklist

Run end-to-end against a real HA + Casa add-on before tagging a release. The
legacy synchronous matrix was last verified 2026-07-11 against HA Core 2026.6.4,
HACS 2.0.5, and Casa add-on 0.65.2. The 0.3.0 background path remains dark until
its separate live Voice PE acceptance matrix is completed.

1. HACS install completes. Config flow accepts valid secret; rejects invalid secret with the `Invalid webhook secret` error.
2. Assign Casa Butler as pipeline conversation agent. Voice turn from a satellite flows STT → Casa → TTS with audible butler reply.
3. Trigger a satellite wake with debug logging enabled for `custom_components.casa`. HA logs `Registering voice session scope=<device_id>` before the utterance arrives. (Casa 0.4x+ registers the scope for idle-sweep/dedup only; it no longer prewarms memory on `stt_start`.)
4. Barge in mid-TTS. Casa receives a `cancel` frame for the in-flight utterance; HA recovers; next turn succeeds.
5. Disconnect Casa mid-conversation. Satellite speaks the fallback line, HA does not go silent, reconnect recovers without integration reload.
6. Switch **Transport** to `sse` via Options. Integration reloads. One full turn works (no session registration, expected).
7. Change the webhook secret on Casa without updating HA. Next turn triggers reauth; entering the new secret restores normal operation.
8. Against a protocol-1 Casa route, complete one background job while the
   satellite is busy and verify it is announced only after stable idle. Drop
   the WebSocket before the delivered acknowledgement and verify an ordinary
   reconnect claims and acknowledges the reoffer without replay. Repeat with
   an integration restart before the reoffer and verify the summary may replay
   once but is never silently lost.

## Architecture

See [phase 2.4 design spec](https://github.com/bonzanni/casa-ha-app/blob/master/docs/superpowers/specs/2026-04-17-ha-integration-2.4.md) for full design rationale. The [voice-pipeline 2.3 spec](https://github.com/bonzanni/casa-ha-app/blob/master/docs/superpowers/specs/2026-04-17-voice-pipeline-design.md) is the wire-contract source of truth.

## Limitations (2.4)

- Voice-ID speaker promotion lands when HA exposes stable `user_input.context.user_id` — no integration change needed.
- SSML output blocked by HA's Assist pipeline; plain tag dialects only.
- No pytest-HA harness tests — manual E2E above is the gating bar. Queued for 2.4.1.

## License

MIT.
