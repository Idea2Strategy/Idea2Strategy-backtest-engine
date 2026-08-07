# Backtest Engine 개발 가이드

`README.md` 와 마찬가지로 이 문서는 **현재 사실**만 기술합니다. 예정된 것은 "예정"이라고
명시합니다.

## 저장소 경계

| 스키마 | 이 저장소의 권한 | 현재 코드 상태 |
|---|---|---|
| `backtest` | write | 9개 테이블 전부 `src/backtest_engine/persistence` 에서 read/write |
| `storage` | read only | 시장 데이터·feature 오브젝트 메타데이터를 읽습니다. write는 거부합니다 |
| `market_data`, `strategy` | read only | 고정된 dataset, feature materialization, compiled plan을 운영 어댑터가 읽습니다 |
| 그 외 (`identity`, `bot`, `competition`, `performance`, `trading`, `operations`) | 접근 없음 | 런타임 가드가 write를 거부합니다 |

`storage` 소유권은 **미해결 모순**입니다. 체크리스트는 D 소유라고 하고
`DatabaseAccessPolicy.java:36` 은 `SHARED` 로 등록합니다. 그래서 이 저장소는 `storage`
스키마를 기여 루트에서 주장하지 않고, `storage` DDL을 작성하지 않으며, 오브젝트 행도
만들지 않습니다. 자세한 내용은 `db/migration-contributions/README.md` 를 보십시오.

## 재생·체결 불변식

오케스트레이터는 입력을 임의의 현재 상태로 다시 조회하지 않습니다. 요청에 고정된 봇
스냅샷, compiled plan, dataset hash, feature result hash, 실행 정책 버전을 검증한 후에만
재생합니다. 고정 증거가 없거나 바뀌면 fail-closed로 `UNAVAILABLE` 또는 실패 처리합니다.

지원 주기는 `30m`, `1h`, `4h`, `1d`이며 기존 `1m`, `5m`, `15m`도 호환성을 위해
유지합니다. shorthand와 계약 값의 대응은 다음과 같습니다.

| shorthand | 계약 값 | 고정 기간 |
|---|---|---|
| `30m` | `PT30M` | 30분 |
| `1h` | `PT1H` | 1시간 |
| `4h` | `PT4H` | 4시간 |
| `1d` | `PT24H` | 24시간(계약/워밍업 계산용) |

`1d`의 실제 시장 이벤트는 24시간 봉이 아니라 `session_date_et`에 해당하는 XNYS 정규장
시가부터 종가까지입니다. 4시간 봉의 마지막 구간도 세션 종가에서 끝날 수 있습니다. 이처럼
명목 기간보다 짧은 봉은 데이터 오류와 구분하기 위해 반드시 `session_truncated=true`여야
합니다.

평가는 `BAR_CLOSED`에서 수행하지만 같은 봉에 주문을 소급 체결하지 않습니다. 장중 봉에서
생긴 주문은 다음 체결 가능 이벤트부터, 세션 종가에 생긴 주문은 다음 정규장 시가부터
유효합니다. 데이터 누락 구간은 주문 체결 가능 시점으로 간주하지 않습니다. 이 규칙은
look-ahead bias를 막는 재현성 계약의 일부입니다.

## 실행과 취소 흐름

worker는 SQS 메시지마다 안정적인 `worker_execution_key`를 만들고 DB CAS로 시도를
획득합니다. 실행 중에는 visibility timeout을 연장하며 결과에 따라 다음과 같이 처리합니다.

| 결과 | 실행 상태 | 큐 처리 |
|---|---|---|
| 성공 | `COMPLETED` | ack/delete |
| 일시 오류 | 재시도 가능 상태 | visibility 반환, 한도 초과 시 DLQ |
| 영구 오류 | `FAILED` 또는 `UNAVAILABLE` | DLQ |
| 사용자/조정자 취소 | `CANCELLED` | ack/delete, DLQ 금지 |

취소 결과 이벤트는 `BACKTEST_CANCELLED`, `cancelledAt`, `attempt`, `reasonCode`를 포함합니다.
lifecycle과 영속성 계층은 기존 취소 요청 시각·사유가 있으면 이를 보존하고, 없을 때만 결과
이벤트 값으로 채웁니다. 이미 취소된 실행의 재전달은 중복 종단 상태로 ack합니다.

## DB 접근 규칙

- **SQLAlchemy Core만** 사용합니다. ORM 세션, declarative 모델, Alembic을 쓰지 않습니다
  (체크리스트 공통 완료 기준).
- psycopg는 SQLAlchemy의 드라이버로만 씁니다. raw psycopg를 직접 쓰지 않습니다.
- **런타임은 DDL을 실행하지 않습니다.** `create_backtest_engine()` 이 만든 엔진은
  `CREATE`/`ALTER`/`DROP`/`TRUNCATE`/`GRANT`/`REVOKE`/`COMMENT` 를 커서 단계에서 거부하고,
  `backtest` 외 스키마로의 `INSERT`/`UPDATE`/`DELETE` 도 거부합니다. 마이그레이션 실행은
  중앙 Flyway 번들(`backend/db-migration`, A 소유)의 일회성 배포 단계에 속합니다.
- 기동 시 `BacktestPersistence.verify_schema()` 로 정합을 확인하고, drift가 있으면
  문제 목록을 전부 담아 `SchemaDriftError` 로 즉시 실패합니다. 스스로 고치지 않습니다.

## 트랜잭션 경계

`BacktestPersistence.unit_of_work()` 블록 하나가 트랜잭션 하나입니다. 블록 안에서 넘겨받는
모든 리포지토리는 같은 커넥션을 공유합니다.

```python
with persistence.unit_of_work() as uow:
    publish_completed_run(uow, publication)
```

완료 실행 publish는 `performance_summaries`, `monthly_judgment_summaries`,
`failure_condition_counts`, `detail_manifests`, `runs` 다섯 테이블을 건드립니다. 중간에
무엇이 실패하든 전부 롤백됩니다. 부분 publish로 같은 주 파티션에 매니페스트가 두 개
남는 상태는 만들 수 없습니다.

## 동시성 제어

프로세스 안의 락이 아니라 정본 유니크 제약을 씁니다.

| 제약 | 막는 것 |
|---|---|
| `runs.idempotency_key` | 같은 요청의 중복 실행 생성. 동일 요청은 기존 행을 돌려줍니다 |
| `run_attempts.worker_execution_key` | **프로세스를 넘는** 중복 워커. 두 번째 워커는 행을 만들 수 없습니다 |
| `run_attempts (run_id, attempt_number)` | 시도 번호 분기 |
| `input_bundles.run_id` | 실행당 재현성 경계 2개 |
| `monthly_judgment_summaries (run_id, et_year_month)` | ET 월 요약 중복 |
| `detail_manifests (run_id, record_type, week_start_date, part_number)` | 같은 주 파티션 중복 |
| `detail_manifests.object_id` | 오브젝트 하나에 매니페스트 2개 |

상태 전이는 조건부 `UPDATE ... WHERE status IN (...)` 으로 수행합니다. 이미 같은 종단
상태면 at-least-once 재전달로 보고 그대로 돌려줍니다.

## 금액 정밀도

정본 금액 컬럼은 `numeric(24,8)` 입니다. PostgreSQL은 스케일을 넘는 값을 **말없이
반올림**하므로, 영속성 경계는 반올림하지 않고 `MoneyPrecisionError` 로 거부합니다.
양자화는 `money.py` 의 책임이며, 재현 해시는 양자화 후 값으로 계산합니다.

## 마이그레이션

- 정본 모델은 루트 `db/schema.dbml` 입니다.
- 이 저장소의 기여 루트는 `db/migration-contributions/` 이고 `owner=backtest`,
  `schemas=backtest` 입니다.
- 파일명은 `V<YYYYMMDDHHMMSS>__backtest_<slug>.sql` 이어야 합니다. `V001__` 같은 legacy
  번호는 중앙에서 거부됩니다.
- 9개 테이블의 baseline은 중앙 `V1__initial_schema.sql` 에 있습니다. 이 저장소의
  `migrations/`에는 이후 승인된 backtest 전용 변경만 있으며 적용된 파일은 수정하지 않습니다.
- 적용된 정본 번들의 바이트 단위 사본이
  `db/migration-contributions/fixtures/central-migration/` 에 있고, 통합 테스트가 이것을
  적용합니다. 사본이 손대졌거나 중앙 번들이 바뀌면
  `tests/persistence/test_central_migration_fixture.py` 가 실패합니다.

## 테스트

```text
python -m pytest -p no:cacheprovider                # 단위 (Docker 없음, 기본값)
python -m pytest -p no:cacheprovider -m docker      # Testcontainers PostgreSQL 16
python -m ruff check src tests
python -m mypy
```

- `docker` 마커가 붙은 테스트만 컨테이너를 씁니다. 기본 실행은 `-m 'not docker'` 입니다.
- 통합 테스트는 **손으로 쓴 DDL을 쓰지 않습니다.** 정본 Flyway 번들을 그대로 적용해,
  코드가 정본 스키마와 맞는지를 증명합니다.
- 상위 도메인 FK가 필요한 참조 행은
  `db/migration-contributions/fixtures/backtest_reference_seed.sql.fixture` 에 있습니다.
  테스트 전용 데이터이며 시드도 마이그레이션도 아닙니다.
- 테스트 먼저 씁니다. 카드마다 실패하는 테스트를 먼저 커밋합니다.

## 고정 정책

- 슬리피지 0.05%, 수수료 0.2%. `backtest.runs` 는 `fee_policy_id`,
  `buying_power_buffer_policy_id`, `slippage_rate_bps`, 그리고 세 개의 rules version을
  실행마다 고정합니다.
- 실행 상태는 정본 `backtest.run_status` =
  `QUEUED|RUNNING|COMPLETED|FAILED|CANCELLED|UNAVAILABLE` 입니다. `COMPLETE` 는 정본
  라벨이 아닙니다.
- 상세 Parquet 파티션은 **ET 월요일 주 경계 + `part_number`**, 압축은 명시적
  `UNCOMPRESSED` 입니다.

## Git Flow

- `develop`이 기본 개발 브랜치입니다.
- `feature/*`, `fix/*`, `docs/*`, `chore/*`는 `develop`에서 시작해 `develop`으로 병합합니다.
- `release/*`로 정식 릴리스를 준비합니다.
- `main`에는 v1.0.0부터 검증된 정식 릴리스만 병합합니다.
- 정식 릴리스 이후 `hotfix/*`는 `main`과 `develop` 모두에 반영합니다.
