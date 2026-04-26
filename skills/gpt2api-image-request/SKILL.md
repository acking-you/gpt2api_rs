# GPT2API Image Request

Use when a user needs to request, poll, retrieve, or debug image generation
through the public GPT2API service on `ackingliu.top`.

## Contract

This skill is for downstream users who already have a GPT2API key. It is not an
operator/admin workflow.

- Public API base: `https://ackingliu.top/api/gpt2api`
- Browser UI: `https://ackingliu.top/gpt2api/login`
- Auth: `Authorization: Bearer <GPT2API_KEY>`
- Default image model: `gpt-image-2`
- Default image size: `1024x1024`
- Image count `n`: integer from `1` to `4`
- Size format: `WIDTHxHEIGHT`; each side must be between `256` and `4096`
  pixels. `auto` or an omitted size resolves to `1024x1024`.

Do not use service-admin tokens for public requests. Do not use local
`127.0.0.1` URLs unless explicitly debugging the service host.

## Preflight

Set the key and base URL:

```bash
export GPT2API_BASE="https://ackingliu.top/api/gpt2api"
export GPT2API_KEY="replace-with-user-key"
```

Verify the key:

```bash
curl -fsS "$GPT2API_BASE/auth/verify" \
  -H "Authorization: Bearer $GPT2API_KEY"
```

List available models:

```bash
curl -fsS "$GPT2API_BASE/v1/models" \
  -H "Authorization: Bearer $GPT2API_KEY"
```

## Generate One Image

Use the OpenAI-compatible image endpoint:

```bash
curl -fsS "$GPT2API_BASE/v1/images/generations" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-image-2",
    "prompt": "A clean product photo of a ceramic coffee cup on a wooden desk",
    "n": 1,
    "size": "1024x1024"
  }' \
  -o image-response.json
```

Decode the first returned image:

```bash
jq -r '.data[0].b64_json' image-response.json | base64 -d > image.png
```

The response shape is OpenAI-compatible:

```json
{
  "created": 1770000000,
  "data": [
    {
      "b64_json": "...",
      "revised_prompt": "..."
    }
  ]
}
```

This endpoint waits for task completion and returns base64 image data. If the
queue may be slow, use the asynchronous session workflow below instead.

## Edit One Image

Use multipart form data:

```bash
curl -fsS "$GPT2API_BASE/v1/images/edits" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -F model=gpt-image-2 \
  -F prompt="Keep the same subject, but change the background to a rainy street" \
  -F n=1 \
  -F size=1024x1024 \
  -F image=@input.png \
  -o edit-response.json
```

Decode the image the same way:

```bash
jq -r '.data[0].b64_json' edit-response.json | base64 -d > edited.png
```

## Conversation-Aware Requests

The compatible endpoints keep a key-scoped API session automatically. To bind
multiple calls into a specific remembered conversation, pass
`x-gpt2api-session-id`.

Create a session:

```bash
curl -fsS "$GPT2API_BASE/sessions" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"title":"Shenzhen skyline image set"}' \
  -o session.json
```

Use the returned session id with direct generation:

```bash
SESSION_ID="$(jq -r '.session.id' session.json)"

curl -fsS "$GPT2API_BASE/v1/images/generations" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -H "x-gpt2api-session-id: $SESSION_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-image-2",
    "prompt": "Continue the same visual direction, now at night with neon reflections",
    "n": 1,
    "size": "1536x1024"
  }' \
  -o continued-response.json
```

For fully asynchronous UI-style requests, append a message to a session:

```bash
curl -fsS "$GPT2API_BASE/sessions/$SESSION_ID/messages" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "image_generation",
    "prompt": "Continue the previous image, but make it a wide cinematic frame",
    "model": "gpt-image-2",
    "n": 1,
    "size": "2048x1024"
  }' \
  -o task.json
```

For asynchronous image edits in the same conversation, use multipart form data:

```bash
curl -fsS "$GPT2API_BASE/sessions/$SESSION_ID/messages/edit" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -F model=gpt-image-2 \
  -F prompt="Use the previous style and turn this reference into a poster" \
  -F n=1 \
  -F size=1024x1024 \
  -F image=@input.png \
  -o edit-task.json
```

Then observe progress and retrieve artifacts:

```bash
TASK_ID="$(jq -r '.task.id' task.json)"

curl -fsS "$GPT2API_BASE/tasks/$TASK_ID" \
  -H "Authorization: Bearer $GPT2API_KEY"

curl -fsS "$GPT2API_BASE/sessions/$SESSION_ID" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -o session-detail.json

ARTIFACT_ID="$(jq -r '.artifacts[-1].id' session-detail.json)"
curl -fsS "$GPT2API_BASE/artifacts/$ARTIFACT_ID" \
  -H "Authorization: Bearer $GPT2API_KEY" \
  -o artifact.png
```

For live task updates, connect to the SSE stream:

```bash
curl -N "$GPT2API_BASE/sessions/$SESSION_ID/events" \
  -H "Authorization: Bearer $GPT2API_KEY"
```

## Compatibility Endpoints

Prefer `/v1/images/generations` and `/v1/images/edits` for direct image work.
For clients that only support text-style OpenAI APIs, GPT2API also accepts
image-shaped requests through:

- `POST /v1/chat/completions`: set `model` to `gpt-image-2` or include
  `"modalities":["image"]`. The response puts base64 data URLs in
  `choices[0].message.content`.
- `POST /v1/responses`: include an `image_generation` tool through `tools` or
  `tool_choice`. The response puts base64 image data in `output[].result`.

These compatibility endpoints use the same Bearer key, size rules, queue, and
usage ledger as the direct image endpoints.

## Cost Notes

Generation credits are recorded on the key usage ledger.

- Direct image credit units are based on area:
  `ceil(width * height / (1024 * 1024))` per image.
- `1024x1024` costs `1` unit per image.
- `1536x1024` or `1024x1536` costs `2` units per image.
- `2048x2048` costs `4` units per image.
- Conversation-aware session requests may add context surcharge for prior text
  and image context used by the same session.

Use the user credit page in the web UI for a user-facing view. Use admin usage
logs only when acting as an operator.

## Troubleshooting

- `401`: missing or invalid Bearer key.
- `403`: key disabled or quota exhausted.
- `400`: bad JSON, bad multipart form, invalid `n`, invalid `size`, or a
  session id that does not belong to the key.
- `502`: upstream image service rejected or failed the request.
- Empty or invalid `b64_json`: keep `image-response.json` and inspect the full
  error body before retrying.
