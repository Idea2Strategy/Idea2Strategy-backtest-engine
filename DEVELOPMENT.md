# Backtest Engine 개발 가이드

`README.md` 와 마찬가지로 이 문서는 **현재 사실**만 기술합니다. 예정된 것은 "예정"이라고
명시합니다.

## 저장소 경계

| 스키마 | 이 저장소의 권한 | 현재 코드 상태 |
|---|---|---|
| `backtest` | write | 9개 테이블 전부 `src/backtest_engine/persistence` 에서 read/write |
| `storage` | read only | `StorageObjectReader` 만 존재. write 메서드 없음 |
| `market_data`, `strategy` | read only | **아직 읽는 코드가 없습니다.** 입력 잠금은 `backtest.input_*` 에 해시만 고정합니다 |
| 그 외 (`identity`, `bot`, `competition`, `performance`, `trading`, `operations`) | 접근 없음 | 런타임 가드가 write를 거부합니다 |

`storage` 소유권은 **미해결 모순**입니다. 체크리스트는 D 소유라고 하고
`DatabaseAccessPolicy.java:36` 은 `SHARED` 로 등록합니다. 그래서 이 저장소는 `storage`
스키마를 기여 루트에서 주장하지 않고, `storage` DDL을 작성하지 않으며, 오브젝트 행도
만들지 않습니다. 자세한 내용은 `db/migration-contributions/README.md` 를 보십시오.

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
- 지금 `migrations/` 는 비어 있습니다. 9개 테이블이 이미 적용된 baseline
  `V1__initial_schema.sql` 에 있고, 적용된 마이그레이션은 수정하지 않습니다.
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
- 실행 상태는 정본 `backtest.run_status` = `QUEUED|RUNNING|COMPLETED|FAILED|UNAVAILABLE`
  입니다. `COMPLETE` 는 정본 라벨이 아닙니다.
- 상세 Parquet 파티션은 **ET 월요일 주 경계 + `part_number`**, 압축은 명시적
  `UNCOMPRESSED` 입니다.

## Git Flow

- `develop`이 기본 개발 브랜치입니다.
- `feature/*`, `fix/*`, `docs/*`, `chore/*`는 `develop`에서 시작해 `develop`으로 병합합니다.
- `release/*`로 정식 릴리스를 준비합니다.
- `main`에는 v1.0.0부터 검증된 정식 릴리스만 병합합니다.
- 정식 릴리스 이후 `hotfix/*`는 `main`과 `develop` 모두에 반영합니다.
