# Frozen Session prompt history

The signed-in Session owner can request the frozen prompt for the currently selected Session history:

```text
GET /api/v1/sessions/{session_id}/history/prompt?history_lineage=<lineage>&epoch=<epoch>
```

Pass `history_view.lineage` and `history_view.epoch` from the current Session history view. Both parameters are required. This is an on-demand HTTP read, not part of a WebSocket snapshot or event. The response uses `Cache-Control: no-store`.

On success, the response is shaped as follows:

```json
{
  "session_id": "session-id",
  "history_lineage": "selected-lineage",
  "epoch": 2,
  "status": "available",
  "system": "The exact rendered SYSTEM text recorded in the checkpoint.",
  "tools": [{"name":"example","description":"Example tool","parameters":{"type":"object"}}]
}
```

Tool entries are only the exact declared model-facing definitions, in frozen order. They do not describe permissions, conditions, implementation availability, or whether a tool may be executed. The endpoint does not include settings/templates, variables, transcript, or provider request/audit data.

If the selected checkpoint does not prove the materialized epoch's prompt (for example, the checkpoint is missing or its epoch is pending), HTTP 200 returns the same identity fields with `status: "unavailable"` and `reason: "missing_evidence"`, omitting both `system` and `tools`. If the complete JSON would exceed 256 KiB, the reason is `"response_limit"`; there is no truncated projection. Storage verification failures are errors rather than empty catalogs.

If history selection or epoch changed, the endpoint returns HTTP 409 with `error.code: "stale_history"`; obtain a new history view before reading again. A Session that does not exist or is not owned by the caller returns HTTP 404. A read does not activate a dormant Session or reconstruct an earlier epoch from current settings. Use existing history navigation to select a different checkpoint before requesting its prompt.
