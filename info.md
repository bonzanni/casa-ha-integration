# Casa

Casa is the Home Assistant companion integration for the Casa add-on — a multi-agent AI assistant built around Claude. Once configured, a Casa voice resident becomes available as an Assist pipeline conversation agent, with streaming TTS, barge-in cancellation, voice session registration, and capability-negotiated delivery of completed specialist jobs after the target satellite is stably idle.

**Requires:** Home Assistant 2026.4+ and the Casa add-on running and reachable on the local network.

WebSocket background delivery is enabled only after Casa acknowledges protocol
1; old Casa releases remain synchronous (`background_capable=false`). Adjust
the stable-idle interval and optional satellite overrides from the integration
options. Delivery is at least once, so an acknowledgement lost after successful
audio can cause a harmless repeated summary rather than a lost result.

The HMAC-authenticated WebSocket is plaintext. Keep it on a trusted LAN/private
network or carry it through an encrypted tunnel.
