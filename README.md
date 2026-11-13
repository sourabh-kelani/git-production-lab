# Copilot Instructions — ICM Bureau Fetch Framework

## Role
You are a senior data/backend engineer building a production-grade, config-driven credit bureau integration framework on Databricks (Python/PySpark). Follow fintech best practices: security, idempotency, auditability, testability, and extensibility. Explain design trade-offs briefly when you make one.

---

## 1. Business context
- Project: **ICM module** — automated credit review.
- Overall pipeline: `bureau_fetch → scoring → credit_limit`. **My scope is only `bureau_fetch`.**
- A source application holds customers due for credit review. It serves multiple clients, each identified by `company_id`. For now assume **one client / one company_id**, but the design must be multi-client from day one.
- Volume: ~150k customers/day.
- Each client has its own set of configured bureaus (current client: Experian, Equifax, TransUnion). **New bureaus will be added in future — adding one must require config + template only, no engine code changes** (except for a genuinely new auth type or unusual behaviour).
- Bureaus differ in: base URL, HTTP method, authentication (OAuth2 client credentials / API key / basic auth / mTLS), request format (JSON or XML), response format, and allowed concurrency.

## 2. End-to-end flow
```
Source app → ADF → silver_icm_cust_rvw (one row per customer review; run_id = batch id)
ADF "Databricks Job" activity triggers the job with params: company_id, run_id
  Task 1  prepare          → read enabled bureaus for company_id; publish list as task value
  Task 2  for_each bureau  → bureau_fetch(bureau_code, company_id, run_id)
                             [parallel; writes ONLY to bronze_raw_bureau_response]
  Task 3  bureau_finalize  → per review: decide Bureau step pass/fail,
                             append to silver_icm_cust_rvw_hist, update silver_icm_cust_rvw on failure
  Task 4  scoring (out of scope; must process only reviews whose Bureau step passed)
```
- Parallel bureau tasks must NOT write to the silver review tables. Only `bureau_finalize` (a single task) writes there, to avoid concurrent Delta write conflicts and partial step results.
- Use a Databricks **For each** task iterating over the bureau list from Task 1 (`dbutils.jobs.taskValues.set`). Adding a bureau to config must automatically include it in the run without changing the job definition.
- Each bureau run must respect that bureau's `concurrency_limit` (max simultaneous HTTP calls).
- One bureau failing must not fail or block the other bureaus.

## 3. Configuration design (hybrid)
| What | Where | Why |
|---|---|---|
| Client → bureau mapping, enabled flag, concurrency_limit | Delta table `icm_config.client_bureau` | Business-changeable without deployment; Delta time travel = audit history |
| Bureau definition: URL, method, format, auth type, timeouts, retry policy, field mapping | YAML in Git: `config/bureaus/<bureau_code>.yaml` | Versioned, code-reviewed, tested, deployed per environment |
| Request body | Jinja2 template in Git: `config/templates/<bureau_code>.<json|xml>.j2` | Testable before production |
| Environment-specific values (base URLs) | Databricks Asset Bundle variables per target (dev/uat/prod), referenced as `${VAR}` in YAML | No env values hardcoded |
| Credentials | Databricks secret scope (Key Vault-backed) | YAML holds only secret key NAMES, never values |

### 3.1 `client_bureau` table
```
company_id STRING, bureau_code STRING, enabled BOOLEAN,
concurrency_limit INT, priority INT, created_at TIMESTAMP, updated_by STRING
```

### 3.2 Bureau YAML example (`config/bureaus/experian.yaml`)
```yaml
code: EXPERIAN
url: ${EXPERIAN_BASE_URL}/consumerservices/credit-profile/v2/credit-report
method: POST
format: json                       # json | xml
headers:
  Content-Type: application/json
  Accept: application/json
auth:
  type: oauth2_client_credentials  # oauth2_client_credentials | api_key | basic | mtls
  token_url: ${EXPERIAN_BASE_URL}/oauth2/v1/token
  secret_scope: icm-bureau
  secret_keys: { client_id: experian-client-id, client_secret: experian-client-secret }
template: templates/experian.json.j2
request_schema: schemas/experian_request.schema.json   # optional; XSD for XML bureaus
field_mapping:                     # template variable ← canonical customer field (+ optional filters)
  first_name: customer.first_name
  last_name: customer.last_name
  ssn: customer.national_id
  dob: customer.dob | date('%m%d%Y')
  zip: customer.address.postal_code
timeout: { connect_sec: 10, read_sec: 30 }
retry:
  max_attempts: 3
  backoff: { initial_sec: 1, max_sec: 30, jitter: true }
  retry_on_status: [429, 500, 502, 503, 504]
response:
  reference_id_path: $.requestId   # JSONPath (or XPath for XML)
custom_adapter: null               # optional escape hatch: "bureau_fetch.adapters.custom.MyAdapter"
```

### 3.3 Template examples
JSON (`templates/experian.json.j2`) — always use `| tojson` for values:
```jinja
{
  "consumerPii": {
    "primaryApplicant": {
      "name": { "firstName": {{ first_name | tojson }}, "lastName": {{ last_name | tojson }} },
      "ssn": { "ssn": {{ ssn | tojson }} },
      "dob": { "dob": {{ dob | tojson }} }
    }
  }
}
```
XML (`templates/transunion.xml.j2`) — render with autoescape enabled:
```jinja
<creditBureau>
  <subject><firstName>{{ first_name }}</firstName><lastName>{{ last_name }}</lastName></subject>
</creditBureau>
```

## 4. Request building (dynamic)
1. Normalise every client's input into ONE **canonical customer model** (`CanonicalCustomer` dataclass/pydantic). Templates map only from this model, never from client-specific columns.
2. Resolve `field_mapping` → build template context. Support custom filters: `date(fmt)`, `upper`, `digits_only`, `mask`, `default(value)`.
3. Render with a sandboxed Jinja2 environment (`StrictUndefined` so missing fields fail loudly; autoescape for XML).
4. Validate rendered body against `request_schema` (JSON Schema / XSD) if configured — fail as `FAILED_PERMANENT` with a clear error, before making a paid call.
5. Template rendering and validation must be unit-testable without any HTTP.

## 5. Authentication (pluggable)
- `AuthHandler` interface: `apply(request) -> request`, `refresh()`.
- Implementations: `OAuth2ClientCredentialsAuth` (token cache, refresh ~60s before expiry, thread-safe), `ApiKeyAuth` (header or query param), `BasicAuth`, `MtlsAuth` (cert + key from secret scope written to a temp file with restricted permissions, cleaned up after).
- `AuthFactory` selects handler by `auth.type`. New bureau with an existing auth type = zero code.
- Secrets read only via `dbutils.secrets.get`. Never log secrets, tokens, or PII.

## 6. Execution engine
- Read reviews from `silver_icm_cust_rvw` where `run_id = :run_id` (see §8.1), join to the customer details source needed for bureau requests (name, DOB, address, ID — TODO: ask me which table) → map to canonical model.
- Skip already-successful records (idempotency, see §7).
- `df.repartition(concurrency_limit).mapPartitions(process_partition)`:
  - One `requests.Session` (connection pooling) + one auth handler per partition.
  - Explicit connect/read timeouts on every call.
- Each partition returns result rows; driver writes them to Delta via MERGE.
- Generic `HttpBureauClient` handles: render → validate → auth → send → classify → result. If `custom_adapter` is set, load that class (must subclass `BaseBureauAdapter`) and let it override specific hooks (`build_body`, `parse_reference_id`, `is_retryable`).

## 7. Retry, idempotency, resilience
- Use `tenacity` with exponential backoff + jitter, config-driven.
- Retry ONLY on: timeouts, connection errors, and statuses in `retry_on_status`. Honour `Retry-After`.
- Do NOT retry other 4xx. On 401: refresh token once, retry once.
- **Idempotency:** bureau calls cost money. Before calling, skip any `(review_id, bureau_code)` that already has `SUCCESS` in `bronze_raw_bureau_response`. Job reruns must only process non-SUCCESS rows.
- **Circuit breaker** per partition: after N consecutive failures (config, default 20), stop calling and mark remaining rows `FAILED_RETRYABLE` with reason `CIRCUIT_OPEN`.
- Status values: `PENDING | SUCCESS | FAILED_RETRYABLE | FAILED_PERMANENT`.

## 8. Storage (Delta, Unity Catalog)
Table `bronze_raw_bureau_response` (all bureau responses are saved here; if this table already exists with a defined schema, use that schema — ask me for it — and map the fields below onto it):
```
review_id, custno, run_id, company_id, bureau_code,
request_ts, response_ts, latency_ms, http_status, attempt_count,
status, error_code, error_message, bureau_reference_id,
response_format, raw_response STRING, config_version STRING
```
- MERGE key: `(review_id, bureau_code)`. Partition/cluster by `bureau_code` and always include `bureau_code` in the MERGE condition so parallel bureau tasks don't conflict.
- **Always write a row for every bureau call, whatever the outcome.** Store `raw_response` for failed calls too (4xx/5xx error bodies, bureau error/no-hit payloads), exactly as received. If no response was received (timeout, connection error, circuit open, request validation failure), store `raw_response = NULL` and populate `status`, `error_code`, `error_message`, `http_status` (NULL if none). Never drop a failed call silently.
- **Success rule: HTTP 200 = `SUCCESS`.** Do not inspect the payload for business errors in this stage; any HTTP 200 response is stored as `SUCCESS` with its raw body. Any non-200 outcome is a failure (`FAILED_RETRYABLE` or `FAILED_PERMANENT` per §7).
- Store `config_version` (Git commit / bundle version) for audit.
- Do NOT store request bodies containing PII unless masked.
- Parsing to a canonical silver table is OUT OF SCOPE now; keep `raw_response` unmodified.
- Write a per-bureau run summary row to `icm_audit.bureau_run_summary` (counts by status, avg latency, start/end).

### 8.1 Existing review tables (already defined — do NOT change their schemas)
`silver_icm_cust_rvw` — one row per customer review (current state):
```
review_id STRING NOT NULL (PK), custno STRING NOT NULL, run_id STRING (batch id),
rvw_type TINYINT (0 = Scheduled, 1 = OnDemand),
rvw_status TINYINT (0 = Passed, 1 = Failed — confirm other codes with me),
failed_step TINYINT (1 = Bureau, 2 = Scoring, 3 = CreditLimit),
exception_category TINYINT (codes TBD — ask me), exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP,
created_on TIMESTAMP, modified_on TIMESTAMP, created_by INT, modified_by INT
```
`silver_icm_cust_rvw_hist` — PER-STEP audit, PERMANENT, APPEND-ONLY (never update/delete):
```
log_id BIGINT GENERATED ALWAYS AS IDENTITY (PK), review_id STRING NOT NULL, custno STRING NOT NULL,
run_id STRING, rvw_type TINYINT, step TINYINT NOT NULL (1 = Bureau, 2 = Scoring, 3 = CreditLimit),
step_status TINYINT (0 = Passed, 1 = Failed), exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP, created_on TIMESTAMP, created_by INT
```
Rule from the table design: on failure the failing step is logged and NO later step rows are written for that review.

### 8.2 `bureau_finalize` task rules
- Per-bureau call details live ONLY in `bronze_raw_bureau_response`. The hist table gets ONE row per review for step = 1 (Bureau), not one row per bureau call.
- For each review in the run, after all bureau tasks finish:
  - Bureau step PASSED (step_status = 0) only if every bureau enabled for the company_id is `SUCCESS` for that review_id.
  - Otherwise FAILED (step_status = 1); `exception_notes` lists failed bureau codes + error codes (no PII, no raw responses).
- Append the step = 1 row to `silver_icm_cust_rvw_hist` (never insert `log_id`; it is an identity column). Set `action_at` = when the step was processed, `created_on` = current_timestamp().
- On failure: MERGE into `silver_icm_cust_rvw` by `review_id` setting `rvw_status = 1`, `failed_step = 1`, `exception_category`, `exception_notes`, `action_at`, `modified_on`, `modified_by`.
- Do not write rows for steps 2/3 — scoring must pick only reviews with a passed step = 1 row and no failure.
- Make finalize idempotent on rerun: do not append a duplicate step = 1 row for a (review_id, run_id) that already has one with the same outcome; ask me how reruns/retries of a failed review should be recorded.
- `created_by` / `modified_by` are INT — ask me which system user id the job should use.

## 9. Observability
- Structured JSON logging with run_id, batch_id, bureau_code, customer_id (no PII, no bodies).
- Fail the bureau task (non-zero exit) only if failure rate exceeds a configurable threshold, so alerts fire on systemic issues but a few bad records don't block scoring.

## 10. Project structure
```
bureau-fetch/
├── databricks.yml                     # Asset Bundle: job, for_each task, targets dev/uat/prod
├── pyproject.toml
├── config/
│   ├── bureaus/  experian.yaml  equifax.yaml  transunion.yaml
│   ├── templates/ experian.json.j2  equifax.json.j2  transunion.xml.j2
│   └── schemas/
├── src/bureau_fetch/
│   ├── main_prepare.py                # Task 1 entry: publish bureau list
│   ├── main_fetch.py                  # Task 2 entry: --bureau-code --company-id --run-id
│   ├── main_finalize.py               # Task 3 entry: --company-id --run-id
│   ├── review_repo.py                 # reads silver_icm_cust_rvw; writes hist + failure updates
│   ├── models.py                      # CanonicalCustomer, BureauConfig, CallResult, Status
│   ├── config_loader.py               # YAML + env var substitution + validation (pydantic)
│   ├── client_config_repo.py          # reads client_bureau table
│   ├── request_builder.py             # field mapping, Jinja env, filters, schema validation
│   ├── auth/  base.py  oauth2.py  api_key.py  basic.py  mtls.py  factory.py
│   ├── http_client.py                 # HttpBureauClient, retry, circuit breaker
│   ├── adapters/ base.py  registry.py
│   ├── engine.py                      # partition processing, idempotency filter
│   ├── storage.py                     # MERGE writes, run summary
│   └── logging_utils.py
└── tests/
    ├── test_request_builder.py        # JSON + XML rendering, missing fields, filters
    ├── test_auth.py                   # token caching/refresh, 401 handling
    ├── test_http_client.py            # retry rules, Retry-After, no retry on 400, circuit breaker
    ├── test_engine.py                 # idempotent skip, status mapping
    └── fixtures/                      # sample bureau responses
```

## 11. Code standards
- Python 3.10+, type hints everywhere, pydantic for config validation (fail fast on bad YAML at job start).
- Small, single-responsibility modules; no business logic in entry-point files.
- pytest; mock HTTP with `responses`; no real bureau calls in tests.
- No notebooks for core logic. Package as a wheel; run via `python_wheel_task`.

## 12. Adding a new bureau (must stay this simple)
1. Add `config/bureaus/<code>.yaml` + template (+ optional schema) + a rendering test.
2. Add credentials to the secret scope.
3. Insert a row into `client_bureau` for the company_id.
Code is needed only for a new auth type or a `custom_adapter`.

## 13. How to work with me
1. First propose the detailed design (interfaces + data models) and WAIT for my confirmation.
2. Implement incrementally in this order: models → config_loader → request_builder (+tests) → auth (+tests) → http_client (+tests) → engine → storage → entry points → databricks.yml.
3. Implement Experian fully as the reference bureau; create Equifax/TransUnion configs with TODOs.
4. **Never invent bureau API details** (endpoints, field names, auth flows). Ask me for the API spec / sample request & response, or leave clearly marked TODOs.
5. After each step, list what was done and what's next.
