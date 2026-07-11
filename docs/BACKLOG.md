# Backlog

Cross-repo and follow-up items discovered during the 0.2.0 re-sync (2026-07-11).

## App-side (casa-ha-app — separate session)

These require changes in the Casa add-on repo, not this integration.

1. **Supervisor discovery registration.** The integration ships a full `async_step_hassio`
   discovery flow, but the add-on never announces itself to the Supervisor, so discovery
   never fires — only manual host/port/secret setup works today. Add a startup call that
   registers the add-on with the Supervisor discovery API advertising `host`, `port`, and
   `webhook_secret`. This also resolves the `webhook_secret`-vs-`token` config-key ambiguity
   in `config_flow.py:async_step_hassio` (pick one key and have the integration read it).

2. **`/healthz` version field.** The endpoint returns only `{"status": "ok"}`, so the
   integration has no way to detect app-side contract drift. Add the add-on version (and
   ideally a voice-protocol version) to the payload so the integration can warn on mismatch.
   This is how the two repos drifted invisibly for three months.

3. **Stale prewarm docs.** `casa-agent/DOCS.md:109-110` still claims "On `stt_start`, Casa
   prewarms the voice session's memory cache." The `stt_start` handler stopped calling
   `schedule_prewarm` in the 0.4x memory re-architecture; it now only registers the scope for
   idle-sweep/dedup. Update the doc (or re-wire prewarm if the overlay is ever used at voice's
   clearance again).

## Integration-side (this repo)

4. **pytest-homeassistant-custom-component harness tests.** Current tests run against
   hand-rolled HA stubs in `tests/conftest.py`. Adopt the official test harness so the
   conversation-entity / ChatLog / config-flow contracts are checked against real HA objects.
   Was "queued for 2.4.1"; the satellite-listener bug (fixed in 0.2.0) is exactly the class of
   defect a real harness would have caught, since the stub made the bad `"*"` selector look fine.

5. **WS auto-reconnect.** `WS_RECONNECT_MIN/MAX` constants exist but drive no reconnect loop;
   a dropped socket surfaces as a connection error and the next turn re-establishes. Consider a
   real backoff-driven reconnect if idle disconnects prove disruptive in daily use.
