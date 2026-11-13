# Copilot Instructions — ICM Bureau Fetch

## 0. How to use this file (READ FIRST)
- **Every decision in this file is final. Do NOT ask me questions about anything this file decides.**
- For anything this file does not cover: choose the most conservative option consistent with this file, implement it, mark it in code with a `# DECISION:` comment, and list all such assumptions at the end of your reply. **Do not stop to ask.**
- The ONLY things you may stop and ask me for: bureau API specifications (endpoints, auth flow, request/response samples). Until provided, leave clearly marked `# TODO(bureau-spec):` placeholders and continue.
- Values marked **TODO-CONFIRM** have a working default — implement with the default, keep it in ONE constant/config place so it is easy to change, and continue.

## 1. Role
Senior Python/data engineer building a production-grade credit bureau integration on Databricks. Fintech best practices: security, idempotency, auditability, testability, extensibility. Patterns: Template Method + Adapter + Registry + Strategy.

## 2. Business context
- **ICM module** — automated credit review. Pipeline: `bureau_fetch → scoring → credit_limit`. **Scope: `bureau_fetch` only.**
- A source app holds customers due for review; ADF loads them into `silver_icm_cust_rvw`. Multiple clients (`company_id`), each with its own data schema. Assume one company now; design multi-client from day one.
- ~150k customers/day.
- Each company has a configured list of bureaus (current: Experian, Equifax, TransUnion). New bureaus will be added.
- Typical bureau chain: **AUTH (once per partition) → FETCH (per customer)**. Some bureaus may have more per-customer steps; the design supports an ordered sequence of steps per bureau.

## 3. Core principles
| Python package (code) = BEHAVIOUR | Databricks config tables (DB) = VALUES |
|---|---|
| Base classes, common logic, bureau implementations | Company registry, company → bureaus, enabled |
| Runner: concurrency, retry, circuit breaker, storage | URLs, headers & body templates, step sequence |
| Registry: adapter_code → class | Concurrency, timeouts, retries, min-success rule |
| | Secret key NAMES (values only in Azure Key Vault) |
- URL/template changes in prod = config change, no deployment.
- New bureau = new adapter class (or `GenericRestBureau`) + config rows + secrets.
- **Synchronous HTTP (`requests`) + threads. No asyncio.**
- **HTTP 200 = success.** Anything else = failure. Do not inspect payloads for business errors.
- **All enabled bureaus run in parallel.** `sequence` only orders API calls INSIDE one bureau.

## 4. Repository layout
```
icm-bureau/
├── pyproject.toml                      # one wheel: icm_bureau; console entry points (§9)
├── databricks.yml                      # Asset Bundle: job + targets dev/uat/prod + variables (§9)
├── config/sql/
│   ├── ddl/                            # all CREATE TABLE scripts (§6, §11)
│   └── data/                           # reviewed config INSERT scripts (versioned)
├── src/icm_bureau/
│   ├── core/                           # PURE PYTHON — never import pyspark/dbutils
│   │   ├── models.py                   # CanonicalCustomer, ReviewRef, CompanyConfig, BureauConfig, RequestStep, StepResult, BureauResult, Status, RunSnapshot
│   │   ├── exceptions.py               # BureauError, RetryableError, PermanentError, ConfigError, TemplateError
│   │   ├── constants.py                # status codes, error codes, TODO-CONFIRM defaults (§13)
│   │   ├── template_engine.py
│   │   ├── extractor.py                # JSONPath / XPath
│   │   ├── http_client.py
│   │   ├── auth/ base.py oauth2_client_credentials.py api_key.py basic.py mtls.py factory.py
│   │   ├── bureaus/ base.py json_bureau.py xml_bureau.py generic_rest.py experian.py equifax.py transunion.py
│   │   └── registry.py
│   └── jobs/                           # DATABRICKS LAYER
│       ├── main_prepare.py             # Task 1
│       ├── main_fetch.py               # Task 2 (per bureau)
│       ├── main_finalize.py            # Task 3
│       ├── main_validate_config.py     # config validation utility
│       ├── table_names.py              # fixed table name constants
│       ├── table_resolver.py           # catalog.schema.table resolution
│       ├── config_repo.py              # reads config tables (prepare/validate ONLY)
│       ├── customer_mapper.py          # source rows → CanonicalCustomer
│       ├── snapshot.py                 # run config snapshot + customer snapshot
│       ├── secrets.py                  # driver-side secret resolution
│       ├── runner.py                   # BureauRunner
│       ├── storage.py                  # bronze MERGE
│       └── review_repo.py              # silver review + hist writes (finalize ONLY)
└── tests/ core/ jobs/
```
- Add a test that fails if any `core` module imports `pyspark` or `dbutils`.

## 5. Class design

### 5.1 Rules
1. **Abstract whenever possible**: anything that differs per bureau is an `@abstractmethod`.
2. **Common whenever possible**: anything shared lives ONCE in a base class. Never duplicate logic across bureau classes.
3. Orchestration methods are `@typing.final` (Template Method) and must not be overridden.
4. Bureau classes: receive config/auth/http/templates via constructor; never read DB, never use Spark/dbutils, never log PII; never raise out of `fetch` (convert to results).
5. Format-specific shared logic in `JsonBureau` / `XmlBureau`; auth via `AuthStrategy` (composition).

### 5.2 `AuthStrategy` (core/auth/base.py)
```python
class AuthStrategy(ABC):
    def __init__(self, auth_config: dict, secrets: dict): ...
    @abstractmethod
    def authenticate(self, session) -> None: ...
    @abstractmethod
    def apply(self, request: PreparedRequest) -> PreparedRequest: ...
    def is_expired(self) -> bool: ...            # COMMON, 60s safety margin
    def ensure_valid(self, session) -> None: ... # COMMON, thread-safe (lock)
    def invalidate(self) -> None: ...            # COMMON
```
Implementations: `OAuth2ClientCredentialsAuth`, `ApiKeyAuth`, `BasicAuth`, `MtlsAuth`; `AuthFactory` by `auth_type`. One instance per partition, token shared across threads via lock. Auth is the PER_PARTITION "AUTH" step — not a row in `icm_bureau_request_step`.

### 5.3 `BaseBureau` (core/bureaus/base.py)
```python
class BaseBureau(ABC):
    def __init__(self, config: BureauConfig, auth: AuthStrategy, http: HttpClient, templates: TemplateEngine): ...

    # ABSTRACT — every bureau implements
    @property
    @abstractmethod
    def bureau_code(self) -> str: ...
    @abstractmethod
    def required_fields(self) -> list[str]: ...
    @abstractmethod
    def build_context(self, customer: CanonicalCustomer, review: ReviewRef) -> dict: ...
    @abstractmethod
    def serialize_body(self, rendered_body: str) -> bytes: ...          # done in JsonBureau/XmlBureau
    @abstractmethod
    def parse_payload(self, response) -> Any: ...                       # done in JsonBureau/XmlBureau
    @abstractmethod
    def extract_reference_id(self, step: RequestStep, payload: Any) -> str | None: ...

    # COMMON — final
    @final
    def fetch(self, customer, review, session) -> BureauResult: ...
        # validate_customer → build_context → auth.ensure_valid → run steps by sequence
        # → stop chain on first failure → SUCCESS only if LAST step HTTP 200; log every attempt
    @final
    def validate_customer(self, customer) -> None: ...
    @final
    def execute_step(self, step, ctx, session) -> StepResult: ...
        # render → auth.apply → send(with retry) → classify → extract; 401 → invalidate+ensure_valid → retry once
    def render_url(self, step, ctx) -> str: ...
    def render_headers(self, step, ctx) -> dict: ...
    def render_body(self, step, ctx) -> str | None: ...
    def send(self, request, step, session): ...
    def is_success(self, response) -> bool: ...        # status_code == 200
    def is_retryable(self, response_or_exc) -> bool: ...
    def extract_values(self, step, payload) -> dict: ...
    def to_step_result(self, step, request_ts, response, error) -> StepResult: ...

    # HOOKS — optional, default no-op
    def before_step(self, step, ctx) -> None: ...
    def after_step(self, step, ctx, result) -> None: ...
```
- `JsonBureau(BaseBureau)`: implements `serialize_body`, `parse_payload`, JSONPath extraction.
- `XmlBureau(BaseBureau)`: implements `serialize_body`, `parse_payload` (lxml with external entities disabled), XPath extraction.
- Concrete: `ExperianBureau(JsonBureau)`, `EquifaxBureau(JsonBureau)`, `TransUnionBureau(XmlBureau)` — only bureau-specific logic; API details are `TODO(bureau-spec)`.
- `GenericRestBureau(BaseBureau)`: fully config-driven; `build_context` exposes canonical fields as-is.
- Registry: `@register_bureau("EXPERIAN")`; `get_bureau_class(adapter_code)` raises `ConfigError` if unknown.

### 5.4 Template engine
- Sandboxed Jinja2, `StrictUndefined`. JSON values via `| tojson`; XML with autoescape.
- Context: `customer.*`, `review.*`, values extracted by earlier steps, `{{ secret('key-name') }}`.
- Filters: `date(fmt)`, `upper`, `digits_only`, `b64encode`, `default(v)`, `xml_escape`.
- Templates compiled once per partition.

## 6. Config tables (Delta, shared config schema)
Audit columns on all: `created_by INT, created_on TIMESTAMP, modified_by INT, modified_on TIMESTAMP`.

`icm_company` (ONLY these columns + no audit columns)
```
company_id STRING (PK), company_name STRING, client_id STRING, data_schema STRING,
bureau_min_success_count INT   -- NULL = all enabled bureaus must succeed
```
`icm_bureau_master`
```
bureau_code STRING (PK), bureau_name STRING, adapter_code STRING, auth_type STRING,
auth_config STRING,           -- JSON, non-secret auth settings (e.g. token_url, header name)
is_active BOOLEAN, audit columns
```
`icm_company_bureau`
```
company_id STRING, bureau_code STRING, enabled BOOLEAN,
concurrency_limit INT, threads_per_partition INT (default 10),
circuit_breaker_threshold INT (default 20),
secret_scope STRING, secret_keys STRING (JSON of key NAMES), audit columns
PK: (company_id, bureau_code)
```
`icm_bureau_request_step` — ordered PER_CUSTOMER calls inside a bureau
```
company_id STRING, bureau_code STRING, sequence INT, step_name STRING,
url_template STRING, http_method STRING, headers_template STRING (JSON),
body_template STRING, request_format STRING (JSON|XML|FORM|NONE), response_format STRING (JSON|XML),
extract STRING (JSON {"var": "<JSONPath/XPath>"}), store_response BOOLEAN,
connect_timeout_sec INT (default 10), read_timeout_sec INT (default 30),
max_retries INT (default 3), retry_on_status STRING (default "429,500,502,503,504"),
version INT, is_active BOOLEAN, effective_from TIMESTAMP, audit columns
PK: (company_id, bureau_code, sequence, version)
```

### 6.1 Rules
- `bureau_min_success_count`: validated in Task 1 (`1 <= value <= enabled bureau count`), else ConfigError.
- One active version per `(company_id, bureau_code, sequence)`; never UPDATE active rows — insert new version, switch `is_active`.
- Config is read ONCE in Task 1 and frozen in the run snapshot.

## 7. Table resolution (multi-client)
- `<catalog>.<schema>.<table>`: catalog = bundle variable per target; config schema = bundle variable (default `icm_config`); client data schema = `icm_company.data_schema`.
- `jobs/table_names.py` constants: `silver_icm_cust_rvw`, `silver_icm_cust_rvw_hist`, `bronze_raw_bureau_response`, `CUSTOMER_DETAILS_TABLE` (**TODO-CONFIRM**: default `"silver_icm_customer"`), config table names.
- Client tables (review, hist, bronze, customer details) live in the company's `data_schema`.
- `TableResolver` is the ONLY place table names are built; identifiers validated against `^[A-Za-z0-9_]+$` and backtick-quoted.
- Task 1 verifies every resolved table exists; stores names in the snapshot.
- ADF passes only `company_id`, `run_id` (and optional retry params, §8.5). Never schema/table names.

## 8. Job flow & execution

### 8.1 Job
```
ADF → Databricks Job activity → params: company_id, run_id, [mode, source_run_id]
Task 1 prepare  → icm_company → resolve tables → read ALL config ONCE → validate (incl. registry, secrets keys exist)
                → write config snapshot: /Volumes/<catalog>/<config_schema>/icm_runs/<run_id>/config_snapshot.json (+ config_version hash)
                → build customer snapshot ONCE (reviews to process ⨝ customer details → CanonicalCustomer) → Delta at run-scoped path
                → task value: list of enabled bureau_codes
Task 2 for_each bureau (parallel) → main_fetch --bureau-code {{input}} → writes ONLY bronze (+ logs)
Task 3 finalize → only writer of silver review tables
```

### 8.2 Review selection (Task 1)
- `mode = NORMAL` (default): reviews in `silver_icm_cust_rvw` where `run_id = :run_id`.
- `mode = RETRY` (§8.5): reviews where `run_id = :source_run_id` AND `rvw_status = 1` AND `failed_step = 1`.
- Customer details join: `custno` = customer key (**TODO-CONFIRM** column names; implement in `customer_mapper.py` only).

### 8.3 BureauRunner (jobs/runner.py)
- Concurrency enforced ONLY here: `partitions = ceil(concurrency_limit / threads_per_partition)`; `repartition(partitions).mapPartitions`; `ThreadPoolExecutor(threads_per_partition)` inside.
- Per partition: AuthStrategy, TemplateEngine, HttpClient, bureau instance (registry); **thread-local `requests.Session`**.
- Secrets resolved ONCE on driver; passed in memory; never logged/persisted.
- Idempotency ACROSS ALL run_ids: skip `(review_id, bureau_code)` that already has a bronze row with `status = SUCCESS`. Never re-pull a successful bureau.
- Circuit breaker per partition: after `circuit_breaker_threshold` consecutive failures, stop; remaining customers `FAILED_RETRYABLE`, `error_code = CIRCUIT_OPEN`.
- At the end of each bureau task, log ONE structured summary line (`event=bureau_run_summary`: counts by status, skipped, auth calls/failures, circuit opened, avg/p95 latency, config_version). No summary table.
- Task exit: fail the task only if failure rate > `max_failure_rate_pct` (bundle variable, default 50).
- Comment in code: cluster must cover the SUM of concurrency across bureaus.

### 8.4 Retry & failure rules
- tenacity, exponential backoff + jitter; per-step `max_retries`, `retry_on_status`; honour `Retry-After`.
- Retry only timeouts, connection errors, configured statuses. No retry on other 4xx. 401 → re-auth once, retry once.
- **Every attempt, retry, step and AUTH call is LOGGED (not stored in DB)** — see §12.
- **AUTH failure** (after retries): every customer in the partition → `FAILED_RETRYABLE`, `error_code = AUTH_FAILED`, one bronze row each with `raw_response` = AUTH error body if any else NULL (never a token). Never leave a customer without a bronze row.
- **FETCH (any PER_CUSTOMER step) failure** (after retries): chain stops; bronze row holds that step's failed response body (or NULL + error details).
- No partial-chain resume: on a retry run, a bureau that has not succeeded re-runs its chain from the first PER_CUSTOMER step. (Most bureaus are AUTH → FETCH, so this is a single call.)
- Statuses: `SUCCESS | FAILED_RETRYABLE | FAILED_PERMANENT`.

### 8.5 Retry runs
- A manual retry is a new job run: `mode = RETRY`, new `run_id`, `source_run_id` = original run. Prior attempts are preserved.
- Only bureaus without a final-step SUCCESS (any run) are called.
- Databricks job repair with the SAME run_id overwrites rows of that run.

## 9. Packaging & deployment
- `pyproject.toml` entry points: `icm-prepare`, `icm-fetch`, `icm-finalize`, `icm-validate-config` → `icm_bureau.jobs.main_*:main`.
- `databricks.yml`: `python_wheel_task` per task; for_each over Task 1 task value, `concurrency` = number of bureaus; targets dev/uat/prod; variables: `catalog`, `config_schema`, `snapshot_volume`, `system_user_id`, `max_failure_rate_pct`, job cluster spec (placeholders).
- Python: write code compatible with **Python 3.11** (Databricks Runtime); no newer-only syntax.
- Job runs as a service principal. Deploy via `databricks bundle deploy -t <target>` from CI.

## 10. Credentials
- Values ONLY in **Azure Key Vault**, via a Key Vault-backed Databricks secret scope; one vault/scope per environment.
- **Never store credentials in any table — not even encrypted** — nor in YAML, code, Git, job params, widgets, env vars. Do not generate columns/DDL holding secret values.
- Credentials are per **company + bureau**: mapping in `icm_company_bureau.secret_scope` / `secret_keys`. Naming: `<bureau>-<company_id>-<item>`.
- A run loads only its company's secrets. Strict isolation = separate vault/scope (config change only).
- `secrets.py`: resolve once on driver; on missing key fail fast with key NAME only.
- mTLS: cert/key to temp files `0600`, deleted after partition.
- Never log/print/persist secrets or tokens; test that no secret value appears in logs.

## 11. Storage

### 11.1 `bronze_raw_bureau_response` (client schema) — create if not exists
**One row per (run_id, review_id, bureau_code)** = the FINAL outcome of that bureau for that review in that run.
```
run_id STRING, review_id STRING, custno STRING, company_id STRING, bureau_code STRING,
status STRING,                    -- SUCCESS | FAILED_RETRYABLE | FAILED_PERMANENT
last_step_name STRING,            -- step that produced this response (final step on success, failing step on failure)
http_status INT, attempt_count INT, -- of the last step
error_code STRING, error_message STRING,
bureau_reference_id STRING, response_format STRING,
raw_response STRING,              -- the response body, successful OR failed, exactly as received; NULL if none received
request_ts TIMESTAMP, response_ts TIMESTAMP, latency_ms BIGINT,
config_version STRING, created_on TIMESTAMP
CLUSTER BY (bureau_code, review_id)
```
- Success → final step's response. Failure → failing step's response (or NULL + error fields if no response, e.g. timeout, AUTH_FAILED, CIRCUIT_OPEN).
- Intermediate step responses and individual retry attempts are NOT stored — they are logged (§12).
- `store_response = false` on a step → `raw_response = NULL` if that step is the one recorded.
- Never store tokens, secrets, or request bodies.
- MERGE key `(run_id, review_id, bureau_code)`; include `bureau_code` in the MERGE condition. A retry run (new run_id) adds new rows; earlier runs are kept.

### 11.2 Existing review tables (DO NOT change schemas)
`silver_icm_cust_rvw`
```
review_id STRING NOT NULL (PK), custno STRING NOT NULL, run_id STRING,
rvw_type TINYINT (0 Scheduled, 1 OnDemand), rvw_status TINYINT (0 Passed, 1 Failed),
failed_step TINYINT (1 Bureau, 2 Scoring, 3 CreditLimit),
exception_category TINYINT, exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP, created_on TIMESTAMP, modified_on TIMESTAMP,
created_by INT, modified_by INT
```
`silver_icm_cust_rvw_hist` — append-only, permanent
```
log_id BIGINT GENERATED ALWAYS AS IDENTITY (PK), review_id STRING NOT NULL, custno STRING NOT NULL,
run_id STRING, rvw_type TINYINT, step TINYINT NOT NULL (1 Bureau, 2 Scoring, 3 CreditLimit),
step_status TINYINT (0 Passed, 1 Failed), exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP, created_on TIMESTAMP, created_by INT
```

### 11.3 What each table holds (guarantees)
- `silver_icm_cust_rvw_hist` stores ONLY pipeline step results — 3 possible steps: 1 = Bureau (bureaus called and min-success criteria met or not), 2 = Scoring, 3 = CreditLimit. **This project writes only step 1.** No per-bureau, per-API-step or per-attempt rows — those go to bronze (per bureau) and logs (per attempt). At most one step-1 row per (review_id, run_id).
- `silver_icm_cust_rvw` holds the current overall status of the review (updated by finalize).
- `bronze_raw_bureau_response` has **one row for EVERY bureau actually called for EVERY customer in the run — success or failure, never skipped** (including AUTH_FAILED / CIRCUIT_OPEN / template errors). Bureaus not called because they already succeeded in an earlier run get no new row (their earlier SUCCESS row is reused by finalize). Add a test: rows written per bureau task = customers processed for that bureau.

### 11.4 `bureau_finalize` rules
- `success_count` per review = enabled bureaus with a bronze row `status = SUCCESS` in ANY run (latest row per (review_id, bureau_code)).
- PASSED if `success_count >= bureau_min_success_count` (NULL → all enabled); else FAILED.
- Append ONE hist row per review per run, `step = 1`, `run_id` = current run, `rvw_type` copied from review, `arrival_at` copied from review, `action_at` = processing time, `created_on` = now, `created_by` = `system_user_id`. Never insert `log_id`. No duplicate for same (review_id, run_id).
  - PASSED: `step_status = 0`; `exception_notes` lists any failed bureaus, e.g. `2 of 3 succeeded (min 2); failed: TRANSUNION(HTTP 503)`.
  - FAILED: `step_status = 1`; e.g. `1 of 3 succeeded (min 2); failed: EXPERIAN(HTTP 503), EQUIFAX(TIMEOUT)`. Codes only — no PII/raw data.
- On FAILED → MERGE `silver_icm_cust_rvw` by review_id: `rvw_status = 1`, `failed_step = 1`, `exception_category = EXC_CAT_BUREAU`, `exception_notes`, `action_at`, `modified_on`, `modified_by`. Stop processing: no step 2/3 rows; scoring excludes it.
- On PASSED → MERGE: `rvw_status = NULL`, `failed_step = NULL`, `exception_category = NULL`, `exception_notes = NULL` (clears an earlier failure on retry), `action_at`, `modified_on`, `modified_by`. (**TODO-CONFIRM**: status value for "bureau passed, scoring pending" — default NULL.)
- Scoring input = reviews whose latest step-1 hist row has `step_status = 0`.

## 12. Observability
- **Logs are the audit trail for every attempt.** Structured JSON (std `logging`), one line per event:
  - `step_attempt`: run_id, company_id, bureau_code, review_id, sequence, step_name, attempt_no, http_status, latency_ms, error_code, will_retry
  - `auth_attempt`: run_id, bureau_code, partition_id, attempt_no, http_status, latency_ms, error_code
  - `circuit_open`, `bureau_run_summary` (§8.3), `config_loaded`, `finalize_summary`
- Never log PII, request/response bodies, secrets or tokens.
- **Required ops setup (document in README):** configure job-cluster **log delivery** to a storage location (UC Volume / ADLS) or Azure Log Analytics with a retention period agreed with compliance; otherwise logs are lost when the job cluster terminates.
- All timestamps UTC.

## 13. Defaults (`core/constants.py`) — TODO-CONFIRM values
```
SYSTEM_USER_ID              = from bundle variable system_user_id (default -1)
EXC_CAT_BUREAU              = 1
RVW_STATUS_FAILED           = 1
FAILED_STEP_BUREAU          = 1
CUSTOMER_DETAILS_TABLE      = "silver_icm_customer"
DEFAULT_THREADS_PER_PARTITION = 10
DEFAULT_CIRCUIT_BREAKER     = 20
DEFAULT_MAX_FAILURE_RATE_PCT  = 50
```

## 14. Code standards & tests
- Type hints, `abc.ABC`, `@abstractmethod`, `typing.final`; mypy clean. pydantic for config models.
- pytest; HTTP mocked with `responses`; no real bureau calls.
- Required tests: abstract classes not instantiable; concrete bureaus implement all abstract methods; chain stops on failure; 200 = success; retry/no-retry; 401 re-auth; AUTH failure rows; template rendering (JSON/XML/URL/headers) + StrictUndefined; registry unknown code; runner partition/thread math; cross-run idempotent skip; circuit breaker; one bronze row per (run, review, bureau) with failed body stored; attempt logging; finalize pass/fail with min-success count; hist no-duplicate; table resolver validation; `core` has no pyspark import; secrets never in logs.
- No notebooks for core logic.

## 15. Scope of first version
- Framework + Experian as reference implementation; Equifax/TransUnion classes with `TODO(bureau-spec)`.
- DDL scripts for all new tables. Sample config INSERT scripts with placeholder URLs.

## 16. Working style
1. Propose the design briefly (class diagram, modules), then **continue implementing without waiting** unless something contradicts this file.
2. Order: models/constants/exceptions → auth → template_engine + extractor → http_client → BaseBureau → JsonBureau/XmlBureau → ExperianBureau → registry → table_resolver/config_repo/snapshot/secrets → runner → storage → review_repo/finalize → entry points → databricks.yml → DDL. Tests with each.
3. After each step: what was done, next step, and the `# DECISION:` assumptions list.
