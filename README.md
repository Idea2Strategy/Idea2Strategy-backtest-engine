# Idea2Strategy Backtest Engine

출시된 잠긴 봇 스냅샷을 검증된 시장 데이터로 재현 가능하게 백테스트하기 위한 Python 저장소입니다.

이 문서는 **현재 저장소에 실제로 존재하는 것**만 기술합니다. 아직 구현되지 않은 것은
아래 "아직 구현되지 않은 것"에 그대로 남겨 둡니다.

## 실제 디렉터리 구조

```text
src/backtest_engine/
  api.py                      # FastAPI 앱 (health, POST /backtests, GET /backtests/{id})
  worker.py                   # 프로세스 수명주기 껍데기. 도메인 실행 없음
  contracts.py                # 요청/결과 계약 검증
  execution_policy.py         # 실행 정책 카탈로그
  money.py                    # 금액 정밀도 유틸
  lifecycle.py                # 접수·큐 발행·상태 전이 (in-memory store)
  attempt_coordinator.py      # 시도/리스/재시도 (in-process)
  event_clock.py              # ET 세션 시계
  data_availability.py        # 입력 가용성 판정
  basic_runtime.py            # BASIC 모드 런타임 조각
  execution_model.py          # 주문·체결·비용 모델
  market_data.py              # Parquet 시장데이터 리더
  monthly_judgment.py         # 월별 판정 요약
  result_snapshot.py          # 결과 스냅샷 빌더 (in-memory store)
  detail_object_manifest.py   # 상세 오브젝트 매니페스트 (in-memory store)
  result_query.py             # 소유자 범위 조회 프로젝션 (in-memory store)
  persistence/                # ★ SQLAlchemy Core 영속성 (본 단계에서 신설)
    tables.py                 #   정본 backtest.* 9개 테이블 + storage.objects 메타데이터
    rows.py                   #   테이블별 행 dataclass, numeric(24,8) 검증
    repositories.py           #   리포지토리 + BacktestUnitOfWork
    publish.py                #   완료 실행의 원자적 다중 테이블 publish
    engine.py                 #   엔진 생성, 런타임 가드, 트랜잭션 경계
    schema_guard.py           #   기동 시 스키마 정합 검증 (drift 시 즉시 실패)
    contribution.py           #   db/migration-contributions 계약 리더
    protocols.py              #   후속 단계가 의존할 Protocol과 호환성 한계 기록
db/migration-contributions/   # ★ COM07 기여 루트 (본 단계에서 신설)
tests/
  conftest.py                 # Testcontainers PostgreSQL 16 하네스
  persistence/                # 영속성 단위/통합 테스트
  test_*.py                   # 기존 도메인 테스트
```

Polars 의존성은 없습니다. 표 데이터 처리는 **PyArrow**만 사용합니다.
`apps/api/`, `workers/`, `src/backtest_engine/{strategy_runtime,simulation,order_model,
portfolio,performance,market_data,manifests}/` 는 존재하지 않습니다.

## 지금 실제로 동작하는 것

- `backtest.*` 9개 테이블과 `storage.objects` 에 대한 SQLAlchemy Core 영속성.
  정본 DDL과 컬럼 단위로 대조되며(`tests/persistence/test_table_metadata.py`),
  PostgreSQL 16 컨테이너에 **정본 Flyway 번들을 그대로 적용해** 통합 테스트합니다.
- 멱등성·중복 워커 억제·원자적 publish를 DB 제약으로 강제합니다.
- 런타임은 DDL을 실행할 수 없고, 선언한 `backtest` 스키마 밖으로 write 할 수 없습니다.
- 기동 시 스키마 drift를 감지하면 즉시 실패합니다.
- `GET /health`, `POST /backtests`, `GET /backtests/{id}` 세 엔드포인트 (아직 in-memory store).

## 아직 구현되지 않은 것

| 항목 | 현재 상태 |
|---|---|
| 오케스트레이터 | `src/` 에 없습니다. 12개 모듈을 조립하는 코드는 `tests/d_reproducibility_testkit.py` 안에만 있습니다. |
| Worker | `worker.py` 는 `threading.Event().wait()` 뿐입니다. SQS consumer도 도메인 실행도 없습니다. |
| 오브젝트 스토리지 | S3/로컬 어댑터가 없습니다. `storage.objects` 는 **읽기 전용**이며 행을 만들지 않습니다. |
| 큐 | `SqsBacktestJobQueue` 는 발행만 합니다. 소비자, DLQ, 재전달 처리가 없습니다. |
| API 표면 | `/api/v1` prefix, 인증, 소유자 스코프, 5개 조회 엔드포인트, 결과 수신 엔드포인트가 없습니다. |
| 호출부 전환 | 도메인 모듈은 여전히 `InMemory*Store` 를 씁니다. 영속성 리포지토리로의 교체는 후속 단계입니다. |
| 마이그레이션 SQL | `db/migration-contributions/migrations/` 는 비어 있습니다. 모든 테이블이 이미 정본 baseline에 있습니다. |

## 실행

Python 3.12 (`>=3.12,<3.13`) 전용입니다.

```text
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m pytest                       # Docker 없는 빠른 단위 스위트
.venv\Scripts\python -m pytest -m docker             # Testcontainers PostgreSQL 16 통합
.venv\Scripts\python -m ruff check src tests
.venv\Scripts\python -m mypy
.venv\Scripts\backtest-api                           # http://0.0.0.0:8082
```

기본 `pytest` 실행은 `-m 'not docker'` 로 설정되어 있어 Docker를 건드리지 않습니다.
`-m docker` 를 주면 그 설정을 덮어써 통합 스위트만 실행합니다.

`backtest-worker` 는 아직 종료 신호를 기다리는 것 외에 아무 일도 하지 않습니다.

자세한 경계는 [DEVELOPMENT.md](DEVELOPMENT.md), 마이그레이션 기여 규약은
[db/migration-contributions/README.md](db/migration-contributions/README.md) 를 확인합니다.
