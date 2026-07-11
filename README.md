# Casa HA Integration

Companion custom integration that routes Home Assistant's Assist pipeline through the [Casa add-on](https://github.com/bonzanni/casa-ha-app) — a multi-agent AI assistant built around Claude. Ships streaming TTS deltas, voice session registration via `assist_satellite` state listening, and a "never silent" error path.

## Requirements

- Home Assistant Core 2026.4 or newer.
- Casa add-on running and reachable on the local network.
- HACS (Home Assistant Community Store) installed.

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
- **Session mode** — how memory scope is derived: `device` (default), `user`, `conversation`.
- **Transport** — `ws` (default, enables barge-in + voice session registration) or `sse` (stateless fallback).

## Manual E2E checklist

Before tagging a release, run this end-to-end against the user's real HA + Casa add-on:

1. HACS install completes. Config flow accepts valid secret; rejects invalid secret with the `Invalid webhook secret` error.
2. Assign Casa Butler as pipeline conversation agent. Voice turn from a satellite flows STT → Casa → TTS with audible butler reply.
3. Trigger a satellite wake with debug logging enabled for `custom_components.casa`. HA logs `Registering voice session scope=<device_id>` before the utterance arrives. (Casa 0.4x+ registers the scope for idle-sweep/dedup only; it no longer prewarms memory on `stt_start`.)
4. Barge in mid-TTS. Casa receives a `cancel` frame for the in-flight utterance; HA recovers; next turn succeeds.
5. Disconnect Casa mid-conversation. Satellite speaks the fallback line, HA does not go silent, reconnect recovers without integration reload.
6. Switch **Transport** to `sse` via Options. Integration reloads. One full turn works (no session registration, expected).
7. Change the webhook secret on Casa without updating HA. Next turn triggers reauth; entering the new secret restores normal operation.

## Architecture

See [phase 2.4 design spec](https://github.com/bonzanni/casa-ha-app/blob/master/docs/superpowers/specs/2026-04-17-ha-integration-2.4.md) for full design rationale. The [voice-pipeline 2.3 spec](https://github.com/bonzanni/casa-ha-app/blob/master/docs/superpowers/specs/2026-04-17-voice-pipeline-design.md) is the wire-contract source of truth.

## Limitations (2.4)

- Voice-ID speaker promotion lands when HA exposes stable `user_input.context.user_id` — no integration change needed.
- SSML output blocked by HA's Assist pipeline; plain tag dialects only.
- No pytest-HA harness tests — manual E2E above is the gating bar. Queued for 2.4.1.

## License

MIT.
