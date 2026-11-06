# QuilrAI assessment

Python implementation of the four MCP and LLM gateway tasks. Uses the official MCP SDK, FastAPI, HTTPX, Pydantic, and on-disk SQLite. Customer records and refunds are simulated.

## Run it

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/). The default Python version is 3.12.

```bash
uv sync --locked
uv run pytest -q
uv run python scripts/demo.py
```

No API keys or `.env` are needed for the demo. It starts local mock providers and the gateway, connects to the stdio server through the official MCP client, and checks authorization, redaction, and both fallback paths. It uses a temporary SQLite file and cleans up its processes afterward.

The streaming example produces:

```text
Hello! Email [REDACTED], SSN [REDACTED], card [REDACTED]. Goodbye.
```

The demo also prints time to the first safe text delta. This measures the local mock, not a live provider.

## Code layout

All implementation files are under `src/quilr_gateway/`.

| Task | Main files |
| --- | --- |
| 1. Stdio MCP server | `mcp_server.py`, `stdio_validation.py` |
| 2. Tool authorization proxy | `rpc_proxy.py`, `auth.py` |
| 3. Streaming PII redaction | `redaction.py`, `providers.py` |
| 4. Token budget and fallback | `limiter.py`, `providers.py` |

`app.py` exposes the HTTP endpoints. `mock_upstream.py` supplies the downstream services used by the demo.

## Try the endpoints

For manual HTTP requests, copy `.env.example` to `.env`. Generate a signing secret with the command below and put it in `JWT_SECRET`:

```bash
uv run python -c 'import secrets; print(secrets.token_urlsafe(48))'
```

Start each service in a separate terminal:

```bash
uv run uvicorn quilr_gateway.mock_upstream:app --host 127.0.0.1 --port 8001
uv run uvicorn quilr_gateway.app:create_app --factory --host 127.0.0.1 --port 8000
```

Then issue a local token and call the gateway:

```bash
export VIEWER_TOKEN="$(uv run quilr-token --role viewer --tenant tenant-a)"

# Admin tool: returns -32001, "Unauthorized Tool Call".
curl -s http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key"}}'

# Stream with email, SSN, and card redaction.
curl -N http://127.0.0.1:8000/v1/chat/completions/stream \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":256}'

# Force the mock primary to return 429; the response uses mock-secondary.
curl -s http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"[primary-429]"}],"max_tokens":256}'
```

Generate a token with `--role admin` to allow the admin call. Use `[primary-timeout]` to exercise the 3-second deadline, or `[all-429]` to make both mocks fail. API docs are at <http://127.0.0.1:8000/docs>.

The stdio server runs separately with `uv run quilr-mcp`. An MCP client should launch that command from the repository directory and complete the initialization handshake. Its tools accept:

```json
{"customer_id":"CUST-00001"}
{"customer_id":"CUST-00001","amount":12.5,"reason":"Duplicate payment"}
```

The first input is for `get_customer_record`, the second for `trigger_refund`. `CUST-00001` is the seeded customer.

## Implementation notes

### MCP validation

Customer IDs require five ASCII digits after `CUST-`. Pydantic rejects extra fields and type coercion. Refund amounts must be finite, positive JSON numbers; whole numbers are accepted, strings and booleans are not. Reasons need ten characters after trimming padding.

The SDK is pinned to `mcp==1.26.0`. Its tool decorator turns validation exceptions into `isError` results, so this server uses a low-level handler to return the requested `-32602` protocol error. A small stdin adapter also maps malformed JSON to `-32700`, invalid envelopes to `-32600`, and unsupported methods to `-32601`. The SDK handles initialization, dispatch, and cancellation. Logs go to stderr; stdout is reserved for JSON-RPC.

A valid but missing customer returns a tool execution error. Unexpected execution failures return a sanitized `-32603`.

### Authorization

The gateway verifies the JWT signature, issuer, audience, expiry, subject, and role. It forwards `tools/list` unchanged and checks `admin_` permissions on `tools/call` before contacting the downstream server. Denials preserve the request ID. Client bearer tokens are not forwarded downstream.

The signed subject identifies the tenant for rate limiting, so rotating a token does not reset its budget. This is a POST JSON-RPC proxy to a mock; full MCP Streamable HTTP sessions, GET event channels, SSE relay, and batch requests are outside its scope. Tool calls without IDs are not executed.

### Redaction across chunks

A chunk can end at `alice` and the next can start with `@example.com`. To avoid leaking that prefix, the scanner holds an unfinished word or numeric run and releases completed safe text. Candidate state is limited to 320 characters. Longer candidates are replaced with one marker and discarded through a boundary; this can also redact long benign words.

Patterns cover ASCII emails, `DDD-DD-DDDD` SSNs, and 13–19 digit card-like numbers with optional single spaces or hyphens. Card matching does not require a Luhn checksum, so other long numbers may match. Obfuscated or internationalized addresses need additional detectors.

Provider SSE frames are parsed across byte boundaries and capped at 64 KiB. The client receives `data: {"delta":"..."}` events followed by `data: [DONE]`. Interrupted streams discard the pending candidate and send a sanitized `event: error`, without a success marker. Only one text choice is supported; tool calls and multimodal content are not passed through. Redaction applies to the streaming endpoint.

### Token accounting and fallback

The default limit is 50,000 tokens per tenant per rolling minute. Admission reserves prompt UTF-8 bytes plus a chat-framing allowance and `max_tokens`. Successful provider usage replaces that estimate. Missing usage, errors, or disconnects keep the full reservation.

SQLite `BEGIN IMMEDIATE` makes the budget check and reservation atomic across workers. WAL mode allows concurrent reads. Settled usage expires 60 seconds after completion; pending reservations stay counted until settlement or a 120-second crash-recovery lease expires. Streams have a 60-second total deadline. Charging at completion is conservative for long-running requests.

The primary gets a 3000 ms deadline, including response-body reads for nonstreaming requests. A timeout or HTTP 429 triggers one backup attempt. Other upstream failures return sanitized errors. Streaming fallback stops after the first parsed upstream event to avoid mixing two completions. Local quota rejection returns HTTP 429 with `Retry-After` and makes no provider request.

The byte estimate assumes a byte-level tokenizer and simple text messages. A different tokenizer or chat template needs its own adapter, and the provider must honor `max_tokens`. Actual usage above the reservation is charged in full. Accounting covers logical requests, not combined provider billing for an ambiguous timeout followed by a retry.

## Tests and limits

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

The 61 tests cover stdio output, malformed requests, authorization, every two-chunk split of sample PII, randomized chunking, SSE framing, SQLite concurrency and restart persistence, timeouts, and stream cleanup. GitHub Actions runs the tests and demo on Python 3.11, 3.12, and 3.13.

Live providers have not been tested. Configure their full completion URLs, model names, and API keys in `.env`; the adapter expects chat-completion JSON and SSE ending in `[DONE]`. The mocks should remain on localhost. A deployment needs TLS, private upstream connectivity, and an external identity issuer. Keep SQLite on persistent local storage; a multi-host deployment needs a shared quota service.

Useful references: [pinned MCP SDK](https://github.com/modelcontextprotocol/python-sdk/tree/v1.26.0), [JSON-RPC errors](https://www.jsonrpc.org/specification#error_object), [HTTPX async streaming](https://www.python-httpx.org/async/), and [SQLite transactions](https://www.sqlite.org/lang_transaction.html).
