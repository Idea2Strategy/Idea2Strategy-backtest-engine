# Idea2Strategy Backtest Engine

출시된 잠긴 봇 스냅샷을 검증된 시장 데이터로 재현 가능하게 백테스트하기 위한 Python 저장소입니다.

이 문서는 **현재 저장소에 실제로 존재하는 것**만 기술합니다.

## 실제 디렉터리 구조

```text
src/backtest_engine/
  api.py                      # 인증·소유자 범위를 적용한 /api/v1 HTTP API
  worker.py                   # SQS 소비, 재전달, 실행 키 CAS, DLQ, 취소 ack
  contracts.py                # 요청/결과 계약 검증
  execution_policy.py         # 실행 정책 카탈로그
  lifecycle.py                # 접수·큐 발행·결과 상태 전이
  attempt_coordinator.py      # 시도, 리스, 재시도, 취소 조정
  event_clock.py              # XNYS 정규장 세션 시계
  data_availability.py        # 입력 가용성 판정
  basic_runtime.py            # BASIC 컴파일 계획 평가
  execution_model.py          # 주문·체결·비용 모델
  market_data.py              # Parquet 시장데이터 리더
  feature_outputs.py          # 고정된 feature materialization 검증·로딩
  orchestrator.py             # 결정론적 이벤트 재생과 결과 발행
  wiring.py                   # API/worker/오케스트레이터 조립
  production.py               # PostgreSQL, S3, HTTP 운영 어댑터
  monthly_judgment.py         # 월별 판정 요약
  result_snapshot.py          # 요약·상세 결과 스냅샷 빌더
  object_store/               # 상세 Parquet와 매니페스트 경계
  result_query.py             # 소유자 범위 조회 프로젝션
  persistence/                # SQLAlchemy Core 영속성 및 스키마 가드
```

표 데이터 처리는 **PyArrow**를 사용합니다.

## 비즈니스 로직 흐름

1. API 또는 요청 intake가 봇 스냅샷, 컴파일 계획, 시장 데이터/feature 해시를 고정합니다.
2. worker가 SQS 메시지를 받고 `worker_execution_key` CAS로 중복 실행을 차단합니다.
3. 오케스트레이터가 고정된 Parquet를 읽고 정규장 세션별 `BAR_CLOSED` 이벤트를 만듭니다.
4. BASIC 런타임이 봉 종료 시점까지 알려진 값만 사용해 신호를 평가합니다.
5. 실행 모델이 다음 체결 가능 시점부터 주문을 처리하고 슬리피지·수수료를 적용합니다.
6. 성과, 월별 판정, 상세 Parquet를 만들고 DB와 오브젝트 결과를 발행합니다.
7. `COMPLETED`, `FAILED`, `UNAVAILABLE`, `CANCELLED` 결과를 API로 전달하고 큐 메시지를
   성공, 재시도, DLQ 또는 취소 ack로 마무리합니다.

재현성 경계는 스냅샷 해시, 입력 번들 fingerprint, 실행 정책 버전, 시장 데이터 및 feature
materialization 결과 해시입니다. 같은 입력은 같은 이벤트 순서와 replay digest를 가져야 합니다.

## 지원 봉 주기와 세션 규칙

| 입력 | ISO-8601 | 동작 |
|---|---|---|
| `30m` | `PT30M` | 30분 봉 |
| `1h` | `PT1H` | 1시간 봉 |
| `4h` | `PT4H` | 4시간 봉. 정규장 종료를 넘는 마지막 봉은 장 마감에서 잘립니다 |
| `1d` | `PT24H` | 거래일 정규장 1개를 한 봉으로 취급합니다 |

기존 `1m`, `5m`, `15m`도 호환성을 위해 유지합니다. 모든 주기는 UTC timestamp를 쓰되,
거래 가능 여부와 거래일은 XNYS 정규장(ET) 기준입니다. 공급자가 일봉 시작을 자정으로
표시해도 `session_date_et`의 정규장 시가/종가로 정규화합니다.

4시간 봉처럼 세션 끝에서 짧아진 봉은 `session_truncated=true`인 경우에만 허용합니다.
봉 종료로 생성된 신호는 그 봉에 소급 체결하지 않습니다. 특히 일봉과 세션 마지막 봉의
신호는 다음 정규장 시가부터 주문 체결 대상이 됩니다.

## 구현 경계

- API는 `/api/v1` 접수·목록·상세·시도·성과·월별 요약·상세 매니페스트·입력·결과 수신을
  제공하고 인증 및 소유자 범위를 적용합니다.
- worker는 SQS long poll, visibility heartbeat, 재전달, DLQ, 중복 실행 방지를 구현합니다.
  취소는 실패가 아니므로 DLQ로 보내지 않고 `CANCELLED`로 저장한 뒤 메시지를 삭제합니다.
- 운영 어댑터는 PostgreSQL, S3 versioned object, 결과 수신 HTTP를 연결합니다.
- 런타임은 long-only 주문 모델입니다. 공매도와 정규장 외 체결은 지원하지 않습니다.
- Backtest가 지원하는 주기와 live trading runtime의 지원 주기는 별도 배포 단위입니다.
  동일 전략을 실거래로 전환하려면 trading-engine의 `30m`, `4h`, `1d` 지원 여부를 별도로
  확인해야 합니다.

## 실행

Python 3.12 (`>=3.12,<3.13`) 전용입니다.

```text
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m pytest                       # Docker 없는 빠른 단위 스위트
.venv\Scripts\python -m pytest -m docker             # Testcontainers PostgreSQL 16 통합
.venv\Scripts\python -m ruff check src tests
.venv\Scripts\python -m mypy
.venv\Scripts\backtest-api                           # 환경변수 필요, 아래 참조
```

기본 `pytest` 실행은 `-m 'not docker'` 로 설정되어 있어 Docker를 건드리지 않습니다.
`-m docker` 를 주면 그 설정을 덮어써 통합 스위트만 실행합니다.

### `backtest-api` 환경변수

`backtest_engine.wiring.API_REQUIRED_ENV` 가 정본 목록입니다. 하나라도 없으면 프로세스는
누락된 이름을 **전부 한 번에** 알려주고 기동을 거부합니다. 기본값은 하나도 없습니다.

| 변수 | 값 |
|---|---|
| `BACKTEST_DATABASE_URL` | SQLAlchemy URL |
| `BACKTEST_QUEUE_URL` | 작업 SQS 큐 URL |
| `BACKTEST_API_HOST` / `BACKTEST_API_PORT` | bind 주소 |
| `BACKTEST_AUTHENTICATOR` | `package.module:factory` → `api.Authenticator` |
| `BACKTEST_OBJECT_STORE` | `package.module:factory` → `object_store.ObjectStore` |
| `BACKTEST_OWNER_DIRECTORY` | `package.module:factory` → `lifecycle.OwnerDirectory` |
| `BACKTEST_COMPILED_PLAN_SOURCE` | `package.module:factory` → `lifecycle.CompiledPlanSource` |
| `BACKTEST_DATASET_MANIFEST_SOURCE` | `package.module:factory` → `lifecycle.DatasetManifestSource` |
| `BACKTEST_EXECUTION_POLICY_CATALOG` | `package.module:factory` → `ExecutionPolicyCatalog` |
| `BACKTEST_DEAD_LETTER_SINK` | `package.module:factory` → `lifecycle.DeadLetterSink` |

`AWS_REGION`/`AWS_DEFAULT_REGION` 과 `AWS_ENDPOINT_URL` 은 boto3 자체 설정입니다.

기동 순서는 **구성 → 스키마 검증 → 서비스** 입니다. 런타임은 DDL을 실행하지 않으므로
스키마 drift 는 복구 대상이 아니라 기동 실패 사유입니다.

`backtest-worker` 는 `BACKTEST_QUEUE_URL`, `BACKTEST_DLQ_URL`, `BACKTEST_WORKER_ID`,
`BACKTEST_JOB_HANDLER`, `BACKTEST_EXECUTION_KEY_STORE` 를 모두 요구합니다.
`BACKTEST_EXECUTION_KEY_STORE` 는 더 이상 선택 사항이 아닙니다. 비워 두면 프로세스 지역
딕셔너리(`InMemoryExecutionKeyStore`)로 조용히 대체되어, 이 모듈이 존재하는 이유인
프로세스 간 중복 실행 방지가 사라집니다.

자세한 경계는 [DEVELOPMENT.md](DEVELOPMENT.md), 마이그레이션 기여 규약은
[db/migration-contributions/README.md](db/migration-contributions/README.md) 를 확인합니다.
