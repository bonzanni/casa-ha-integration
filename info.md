# Casa

Casa is the Home Assistant companion integration for the Casa add-on — a multi-agent AI assistant built around Claude. Once configured, a Casa voice resident becomes available as an Assist pipeline conversation agent, with streaming TTS, barge-in cancellation, voice session registration, and capability-negotiated delivery of completed specialist jobs after the target satellite is stably idle.

**Requires:** Home Assistant 2026.4+ and the Casa add-on running and reachable on the local network.

WebSocket background delivery is enabled only after Casa acknowledges protocol
1; old Casa releases remain synchronous (`background_capable=false`). Adjust
the stable-idle interval and optional satellite overrides from the integration
options. After successful audio, an ordinary WebSocket reconnect keeps the
manager's bounded process-local cache and suppresses a replay. If its delivered
acknowledgement was lost, a repeated summary remains possible after a manager or
integration process restart or delivered-cache eviction; at-least-once delivery
prefers that bounded duplicate risk over a silently lost result.

Casa checks an HMAC of the empty HTTP upgrade body to authenticate the
integration client when the WebSocket is established. That handshake does not
authenticate individual frames, and plain `ws://` provides no encryption or
cryptographic server authentication. Keep the connection on a trusted
LAN/private network or carry it through an encrypted tunnel.
