# Private image variables (v0.2)

An authenticated Client can stage an image in an **open interactive root Session**
without a filesystem Agent or a browser plugin. The value is ephemeral in that
runtime; it is not an attachment upload, and there is no byte download route.

## Inspect and mutate

1. Read `GET /api/v1/sessions/{session_id}/state/inspection`. Use the exact
   `witness.runtime_instance_id` and decimal string `witness.variables_revision`.
2. `PUT /api/v1/sessions/{session_id}/variables/bytes/{name}` streams a raw image.
   Use the Session cookie and an allowed `Origin`, one canonical `Content-Type`
   (`image/png`, `image/jpeg`, `image/gif`, or `image/webp`), and one positive
   `Content-Length` matching the body exactly. Send the two witness fields as
   `X-Microcrab-Runtime-Instance-Id` and `X-Microcrab-Variables-Revision`.
   Do not send content encoding or transfer encoding. The upload is subject to
   the Session image-size and aggregate private-byte budgets; Core checks the
   image signature, MIME and dimensions before committing it.
3. `DELETE` on the same path, with the same authentication and witness headers,
   deletes only a byte variable. It accepts no body. With a current witness,
   deleting an absent byte variable is a no-op; it never deletes a JSON variable.

Neither mutation accepts query parameters. PUT returns `{commit, metadata}`;
DELETE returns the commit directly. A commit is `{before, after, changed}`, with
`before` and `after` both containing the two witness fields.

The inspection response has an optional `bytes` object keyed by variable name.
The entries (and the PUT response's `metadata`) contain `name`, `kind: "bytes"`,
`media_type`, numeric `byte_length`, `lifecycle: "ephemeral"`,
`availability: "available"`, decimal string `revision`, and an opaque `selector`.
The map is omitted when empty; it has no tombstones. Neither the legacy State
value/metadata routes nor their JSON-variable sections include these entries.
For the current `view_image`, pass the selector as the whole `image_url` value
in `{{var:base64:@<selector>}}` (replace `<selector>` with the exact opaque value).
It identifies a live value for an explicitly admitted invocation; it is not a
download URL or permission grant. The browser's native State panel copies the
complete `image_url` reference.
An image admitted into model Context has the separate existing retained-image
policy; deleting the variable does not revoke that attachment.

If the request or response is interrupted, or you receive `outcome_unknown`, do
not automatically replay the mutation. Re-inspect live State and decide whether
a new explicit action is needed. A stale witness yields `409 variable_conflict`.
A previous upload acknowledgement is not recoverable from durable operation
receipts, and a closed/replaced runtime or a fork does not carry over the value.
