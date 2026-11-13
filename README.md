# Copilot Instructions — ICM Bureau Fetch (base package + bureau adapters + Databricks job)

## Role
You are a senior Python/data engineer building a production-grade credit bureau integration for Databricks. Follow fintech best practices: security, idempotency, auditability, testability, extensibility. Use clean OOP (Template Method + Adapter + Registry patterns). Explain design trade-offs briefly when you make one.

---

## 1. Business context
- Project: **ICM module** — automated credit review. Pipeline: `bureau_fetch → scoring → credit_limit`. **My scope is only `bureau_fetch`.**
- A source application holds customers due for review. It serves multiple clients, each identified by `company_id`. For now assume **one company_id**, but design for multi-client from day one.
- Volume: ~150k customers/day.
- Each company has its own list of configured bureaus (current: Experian, Equifax, TransUnion). New bureaus will be added later.
- Bureaus differ in authentication, URLs, request format (JSON/XML), and may need **several API calls in a fixed sequence** (e.g. 1 = token → 2 = submit inquiry → 3 = fetch report).

## 2. Design principle: code = behaviour, DB = values
| In the Python package (code) | In Databricks config tables (DB) |
|---|---|
| Base classes, common logic, each bureau's implementation | Company → bureaus, enabled flag |
| Concurrency, retry, circuit breaker, storage (runner) | URLs, headers & request body templates, call sequence |
| Registry: bureau → implementation class | Concurrency limit, timeouts, retry counts |
| | Secret **key names** (never secret values) |

- URL / request changes in prod = config change, **no deployment**.
- A brand-new bureau = new adapter class (deploy) + config rows. A simple REST bureau may use `GenericRestBureau` and need **no new class**.

## 3. Repository layout (two layers in one repo)
```
icm-bureau/
├── pyproject.toml                      # builds ONE wheel: icm_bureau
├── databricks.yml                      # Asset Bundle: job (prepare → for_each bureau → finalize), targets dev/uat/prod
├── config/sql/                         # DDL for config tables + reviewed config insert scripts
├── src/icm_bureau/
│   ├── core/                           # PURE PYTHON — must NEVER import pyspark/dbutils
│   │   ├── models.py                   # CanonicalCustomer, ReviewRef, BureauConfig, RequestStep, StepResult, BureauResult, Status
│   │   ├── exceptions.py               # BureauError, RetryableError, PermanentError, ConfigError, TemplateError
│   │   ├── template_engine.py          # sandboxed Jinja2, filters, secret(), compile cache
│   │   ├── extractor.py                # JSONPath / XPath extraction
│   │   ├── http_client.py              # session factory, timeouts, retry (tenacity), mTLS
│   │   ├── auth/
│   │   │   ├── base.py                 # AuthStrategy (abstract)
│   │   │   ├── oauth2_client_credentials.py
│   │   │   ├── api_key.py
│   │   │   ├── basic.py
│   │   │   └── mtls.py
│   │   ├── bureaus/
│   │   │   ├── base.py                 # BaseBureau (abstract, template method)
│   │   │   ├── json_bureau.py          # JsonBureau(BaseBureau) – common JSON behaviour
│   │   │   ├── xml_bureau.py           # XmlBureau(BaseBureau)  – common XML behaviour
│   │   │   ├── generic_rest.py         # GenericRestBureau – fully config-driven
│   │   │   ├── experian.py             # ExperianBureau(JsonBureau)
│   │   │   ├── equifax.py              # EquifaxBureau(JsonBureau)
│   │   │   └── transunion.py           # TransUnionBureau(XmlBureau)
│   │   └── registry.py                 # @register_bureau("EXPERIAN"), get_bureau_class()
│   └── jobs/                           # DATABRICKS LAYER — Spark, dbutils, Delta I/O
│       ├── main_prepare.py             # Task 1
│       ├── main_fetch.py               # Task 2 (per bureau): --bureau-code --company-id --run-id
│       ├── main_finalize.py            # Task 3
│       ├── main_validate_config.py     # config validation utility
│       ├── config_repo.py              # reads config tables (ONLY used by prepare/validate)
│       ├── snapshot.py                 # write/load run config snapshot + customer snapshot
│       ├── secrets.py                  # resolves secrets on the DRIVER via dbutils.secrets
│       ├── runner.py                   # BureauRunner: concurrency, idempotency, circuit breaker
│       ├── storage.py                  # bronze MERGE, run summary
│       └── review_repo.py              # silver_icm_cust_rvw + hist (finalize only)
└── tests/
    ├── core/                           # fast unit tests, no Spark
    └── jobs/                           # runner/storage tests (local Spark or mocks)
```
- `core` must be unit-testable on a laptop with no Spark. Enforce: add a test that fails if any `core` module imports `pyspark` or `dbutils`.

## 4. Class design (most important section)

### 4.1 Rules
1. **Abstract whenever possible:** anything that genuinely differs per bureau is an `@abstractmethod` that each bureau MUST implement explicitly.
2. **Common whenever possible:** anything that is the same for all bureaus lives ONCE in a base class as a concrete method. Never copy-paste logic into bureau classes.
3. **Template Method:** the orchestration method (`fetch`) is defined in `BaseBureau`, marked `@typing.final`, and must NOT be overridden. Bureaus customise behaviour only through abstract methods and optional hooks.
4. Bureau classes are **stateless per customer**, receive config + secrets via constructor, never read the DB, never use Spark/dbutils, never log PII.
5. Shared behaviour by format goes in intermediate abstract classes (`JsonBureau`, `XmlBureau`); shared auth goes in `AuthStrategy` implementations (composition, not inheritance).

### 4.2 `AuthStrategy` (core/auth/base.py)
```python
class AuthStrategy(ABC):
    def __init__(self, config: dict, secrets: dict): ...
    @abstractmethod
    def authenticate(self, session) -> None: ...          # obtain token/credentials
    @abstractmethod
    def apply(self, request: PreparedRequest) -> PreparedRequest: ...  # add headers / certs
    def is_expired(self) -> bool: ...                     # COMMON: expiry check with safety margin (default 60s)
    def ensure_valid(self, session) -> None: ...          # COMMON: thread-safe (lock) re-auth if expired
    def invalidate(self) -> None: ...                     # COMMON: force re-auth (used on 401)
```
Implementations: `OAuth2ClientCredentialsAuth`, `ApiKeyAuth`, `BasicAuth`, `MtlsAuth`. Selected from config (`auth_type`) via a factory. One instance per partition; token shared across threads with a lock.

### 4.3 `BaseBureau` (core/bureaus/base.py)
```python
class BaseBureau(ABC):
    def __init__(self, config: BureauConfig, auth: AuthStrategy, http: HttpClient,
                 templates: TemplateEngine): ...

    # ---------- ABSTRACT: every bureau must implement ----------
    @property
    @abstractmethod
    def bureau_code(self) -> str: ...

    @abstractmethod
    def required_fields(self) -> list[str]: ...
        # canonical customer fields this bureau needs (validated before any paid call)

    @abstractmethod
    def build_context(self, customer: CanonicalCustomer, review: ReviewRef) -> dict: ...
        # bureau-specific formatting of customer data for templates (date formats, name split, address codes)

    @abstractmethod
    def serialize_body(self, rendered_body: str) -> bytes: ...
        # implemented in JsonBureau / XmlBureau (validate well-formedness, encoding)

    @abstractmethod
    def parse_payload(self, response) -> Any: ...
        # implemented in JsonBureau (json) / XmlBureau (lxml)

    @abstractmethod
    def extract_reference_id(self, step: RequestStep, payload: Any) -> str | None: ...
        # bureau's own request/report id for audit

    # ---------- COMMON: template method — DO NOT OVERRIDE ----------
    @final
    def fetch(self, customer: CanonicalCustomer, review: ReviewRef, session) -> BureauResult:
        # 1. validate_customer → 2. build_context → 3. ensure auth
        # 4. for step in steps sorted by sequence: execute_step; stop chain on first failure
        # 5. return BureauResult (SUCCESS only if LAST step returned HTTP 200)

    # ---------- COMMON: concrete, shared by all bureaus ----------
    @final
    def validate_customer(self, customer) -> None: ...      # uses required_fields(); missing → PermanentError
    @final
    def execute_step(self, step, ctx, session) -> StepResult: ...  # render → apply auth → send → classify → extract
    def render_url(self, step, ctx) -> str: ...
    def render_headers(self, step, ctx) -> dict: ...
    def render_body(self, step, ctx) -> str | None: ...
    def send(self, request, step, session): ...             # retry via http_client using step's retry config
    def is_success(self, response) -> bool: ...             # HTTP 200 == success (project rule)
    def is_retryable(self, response_or_exc) -> bool: ...    # timeouts, conn errors, step.retry_on_status
    def extract_values(self, step, payload) -> dict: ...    # applies step.extract (JSONPath/XPath) into ctx
    def to_step_result(self, step, request_ts, response, error) -> StepResult: ...

    # ---------- HOOKS: optional override, default no-op ----------
    def before_step(self, step, ctx) -> None: ...
    def after_step(self, step, ctx, result: StepResult) -> None: ...
```
- 401 handling (COMMON in `execute_step`): `auth.invalidate()` → `auth.ensure_valid()` → retry once.
- `fetch` and `execute_step` must catch all exceptions and convert them into `StepResult` / `BureauResult` with `FAILED_RETRYABLE` or `FAILED_PERMANENT` — a bureau must never crash the partition.

### 4.4 Format bases (common per format, still abstract)
- `JsonBureau(BaseBureau)`: implements `serialize_body` (json.loads to validate, UTF-8), `parse_payload` (json), JSONPath extraction. Leaves `bureau_code`, `required_fields`, `build_context`, `extract_reference_id` abstract.
- `XmlBureau(BaseBureau)`: implements `serialize_body` (lxml parse to validate), `parse_payload` (lxml, secure parser: no external entities), XPath extraction. Same abstract methods left open.

### 4.5 Concrete bureaus
```python
@register_bureau("EXPERIAN")
class ExperianBureau(JsonBureau):
    bureau_code = "EXPERIAN"
    def required_fields(self): ...
    def build_context(self, customer, review): ...
    def extract_reference_id(self, step, payload): ...
```
- Concrete classes contain ONLY bureau-specific logic. If you find yourself writing the same code in two bureau classes, move it to a base class.
- `GenericRestBureau(BaseBureau)`: config-driven; `build_context` exposes canonical fields as-is; format chosen from config. Use for simple bureaus that need no custom code.

### 4.6 Registry
- `@register_bureau(code)` decorator; `get_bureau_class(adapter_code)` raises `ConfigError` if unknown.
- `icm_bureau_master.adapter_code` selects the class (e.g. `EXPERIAN`, `GENERIC_REST`).
- Task 1 validates that every enabled bureau has a registered class — fail early and clearly.

## 5. Config tables (Delta, Unity Catalog)
Each environment has its own copy, so prod rows hold prod URLs.

`icm_bureau_master`
```
bureau_code STRING, bureau_name STRING, adapter_code STRING, auth_type STRING,
secret_scope STRING, secret_keys STRING (JSON of key NAMES), is_active BOOLEAN, audit columns
```
`icm_company_bureau`
```
company_id STRING, bureau_code STRING, enabled BOOLEAN,
concurrency_limit INT, threads_per_partition INT (default 10),
circuit_breaker_threshold INT (default 20), audit columns
```
`icm_bureau_request_step` — ordered API calls INSIDE a bureau
```
company_id STRING, bureau_code STRING, sequence INT, step_name STRING,
url_template STRING, http_method STRING, headers_template STRING (JSON),
body_template STRING, request_format STRING, response_format STRING,
extract STRING (JSON: {"var": "<JSONPath/XPath>"}), store_response BOOLEAN,
connect_timeout_sec INT, read_timeout_sec INT, max_retries INT, retry_on_status STRING,
version INT, is_active BOOLEAN, effective_from TIMESTAMP, audit columns
```
- Audit columns: `created_by INT, created_on TIMESTAMP, modified_by INT, modified_on TIMESTAMP`.
- `sequence` orders calls within one bureau only. **Bureaus themselves run in parallel.**
- Authentication is handled by `AuthStrategy` (from `auth_type`), not as a step row. Token URL etc. go in `icm_bureau_master` or a JSON `auth_config` column (ask me).

## 6. Databricks job flow
```
Source app → ADF → silver_icm_cust_rvw (run_id = batch id)
ADF "Databricks Job" activity → job params: company_id, run_id

Task 1  prepare          → read ALL config for company_id ONCE; validate; check registry
                           → write config snapshot: /Volumes/<cat>/<schema>/icm_runs/<run_id>/config_snapshot.json (+ config_version hash)
                           → build customer snapshot ONCE (silver_icm_cust_rvw join customer details → canonical model)
                           → publish enabled bureau_codes as task value
Task 2  for_each bureau  → main_fetch(bureau_code)  [PARALLEL; writes ONLY bronze_raw_bureau_response]
Task 3  bureau_finalize  → per review: Bureau step pass/fail → hist + review table
Task 4  scoring          (out of scope)
```
- Config tables are read ONCE per job (Task 1). Bureau tasks read only the snapshot, ONCE, on the driver. **Never query config per customer or per partition.** Mid-run config changes don't affect a running job.
- Customer details source table: TODO — ask me.

## 7. `BureauRunner` (jobs/runner.py) — honours concurrency
```python
class BureauRunner:
    def __init__(self, bureau_code, config_snapshot, secrets, storage): ...
    def run(self, customers_df) -> RunSummary: ...
```
- **Decision: synchronous HTTP (`requests`) + threads. Do NOT use asyncio/aiohttp/httpx-async.** Bureau concurrency limits (tens of calls) cap throughput equally for threads and async, so async adds complexity (async-everywhere base classes, event loop inside Spark, blocking-call stalls) with no gain at this scale. All HTTP goes through the `HttpClient` abstraction so the transport can be swapped later without touching bureau classes.
- **Concurrency is enforced ONLY by the runner, never inside bureau classes.**
- Model: `partitions × threads_per_partition = concurrency_limit`.
  - `partitions = ceil(concurrency_limit / threads_per_partition)`; `customers_df.repartition(partitions).mapPartitions(...)`.
  - Inside each partition: `ThreadPoolExecutor(max_workers=threads_per_partition)`.
  - HTTP calls are I/O-bound, so threads give real concurrency without needing one CPU core per call.
- Per partition: build `AuthStrategy`, `TemplateEngine` (templates compiled once), `HttpClient`, and the bureau instance from the registry using the broadcast config + secrets. Use a **thread-local `requests.Session`** (sessions are not safely shareable across threads); share the auth token via the strategy's lock.
- Secrets: resolve ONCE on the driver (`dbutils.secrets` is unavailable on executors), pass into the partition function. Never log or persist them.
- Idempotency: before calling, anti-join out `(review_id, bureau_code)` that already have a final-step `SUCCESS` in bronze.
- Circuit breaker (shared per partition): after `circuit_breaker_threshold` consecutive failures, stop calling and mark remaining customers `FAILED_RETRYABLE` with `CIRCUIT_OPEN`.
- Returns step results as rows → driver writes via `storage.merge_bronze()`.
- Document in code: all bureau tasks share the job cluster, so cluster size must cover the SUM of bureau concurrency.

## 8. Retry rules (common, in http_client / BaseBureau)
- tenacity, exponential backoff + jitter; per-step `max_retries` and `retry_on_status`.
- Retry ONLY timeouts, connection errors, and configured statuses (default 429, 500, 502, 503, 504). Honour `Retry-After`.
- No retry on other 4xx. 401 → re-auth once, retry once.
- **HTTP 200 = SUCCESS.** Do not inspect payloads for business errors in this stage.

## 9. Storage

### 9.1 `bronze_raw_bureau_response` — one row per step call where `store_response = true`
```
review_id, custno, run_id, company_id, bureau_code, sequence, step_name,
request_ts, response_ts, latency_ms, http_status, attempt_count,
status (SUCCESS | FAILED_RETRYABLE | FAILED_PERMANENT), error_code, error_message,
bureau_reference_id, response_format, raw_response STRING, is_final_step BOOLEAN, config_version STRING
```
- **Always write a row for every stored step call, success or failure.** Store failed response bodies exactly as received. No response (timeout, connection error, circuit open, validation/template error) → `raw_response = NULL` with status and error details.
- Never store tokens, secrets, or request bodies with PII.
- MERGE key `(review_id, bureau_code, sequence)`; cluster by `bureau_code` and include it in the MERGE condition so parallel tasks don't conflict.
- If the existing table schema differs, ask me and map onto it.
- Per-bureau run summary → `icm_audit.bureau_run_summary`.

### 9.2 Existing review tables (do NOT change schemas)
`silver_icm_cust_rvw` — one row per review:
```
review_id STRING NOT NULL (PK), custno STRING NOT NULL, run_id STRING,
rvw_type TINYINT (0 = Scheduled, 1 = OnDemand),
rvw_status TINYINT (0 = Passed, 1 = Failed — confirm other codes with me),
failed_step TINYINT (1 = Bureau, 2 = Scoring, 3 = CreditLimit),
exception_category TINYINT (codes TBD — ask me), exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP, created_on TIMESTAMP, modified_on TIMESTAMP,
created_by INT, modified_by INT
```
`silver_icm_cust_rvw_hist` — per-step audit, PERMANENT, APPEND-ONLY:
```
log_id BIGINT GENERATED ALWAYS AS IDENTITY (PK), review_id STRING NOT NULL, custno STRING NOT NULL,
run_id STRING, rvw_type TINYINT, step TINYINT NOT NULL (1 = Bureau, 2 = Scoring, 3 = CreditLimit),
step_status TINYINT (0 = Passed, 1 = Failed), exception_notes STRING, resolution_note STRING,
arrival_at TIMESTAMP, action_at TIMESTAMP, created_on TIMESTAMP, created_by INT
```
On failure the failing step is logged and NO later step rows are written for that review.

### 9.3 `bureau_finalize` (single task, only writer of silver tables)
- ONE hist row per review for `step = 1`. PASSED only if every enabled bureau has a final-step `SUCCESS`; else FAILED with `exception_notes` = failed bureau codes, step_name, error codes (no PII/raw responses).
- Append hist (never insert `log_id`). On failure MERGE `silver_icm_cust_rvw`: `rvw_status = 1`, `failed_step = 1`, `exception_category`, `exception_notes`, `action_at`, `modified_on`, `modified_by`.
- Idempotent on rerun; ask me how retries of failed reviews should be recorded and which INT user id the job uses.

## 10. Config governance (prod changes without deployment)
- Never UPDATE active step rows in place: insert new `version`, switch `is_active`; rollback = switch back.
- Only a config-admin group can write config tables (Unity Catalog grants); job is read-only.
- `main_validate_config`: renders every template with sample data, checks JSON/XML well-formedness, required placeholders, secret key names, and registry class — run before activating.
- Config changes via reviewed SQL scripts in `config/sql/`. `config_version` stored on every bronze row.

## 11. Code standards
- Python 3.10+, full type hints, `abc.ABC` + `@abstractmethod`, `typing.final` on template/common methods; run mypy.
- pydantic models for config (fail fast in Task 1).
- Structured JSON logging with run_id, bureau_code, sequence, review_id — no PII, bodies, tokens.
- pytest; HTTP mocked with `responses`; no real bureau calls in tests.
- Required tests: abstract classes cannot be instantiated; each concrete bureau implements all abstract methods; `fetch` stops chain on failure; 200 = success; retry/no-retry rules; 401 re-auth; template rendering JSON/XML/URL/headers; StrictUndefined errors; registry unknown code; runner concurrency math; idempotent skip; circuit breaker; finalize pass/fail; `core` has no pyspark import.
- Package as a wheel; Databricks tasks use `python_wheel_task`. No notebooks for core logic.

## 12. Adding a new bureau
1. If it needs custom logic: add `core/bureaus/<name>.py` extending `JsonBureau`/`XmlBureau`, implement only the abstract methods, register it, add tests. Otherwise use `GenericRestBureau`.
2. Insert config rows (master, company_bureau, request_step) via reviewed SQL script.
3. Add secrets. Run `main_validate_config`. Activate.

## 13. How to work with me
1. First propose the detailed design (class diagram, abstract vs common methods, data models, config DDL) and WAIT for my confirmation.
2. Implement incrementally: models + exceptions → auth strategies → template_engine + extractor → http_client → BaseBureau → JsonBureau/XmlBureau → ExperianBureau (reference) → registry → runner → storage → prepare/fetch/finalize entry points → databricks.yml. Tests with each step.
3. **Never invent bureau API details** (endpoints, field names, auth flows). Ask me for specs/samples or leave clearly marked TODOs.
4. After each step, list what was done and what's next.
