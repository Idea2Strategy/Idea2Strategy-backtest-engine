# Backtest Engine 개발 가이드

## 입력과 재현성

- 출시된 잠긴 전략 스냅샷만 입력으로 받습니다.
- `market_data.dataset_manifests`로 고정된 검증 데이터 집합을 참조합니다.
- 필요한 시간 해상도나 데이터가 없으면 다른 해상도로 임의 근사하지 않습니다.
- 입력 Manifest, 엔진 버전, 비용 정책을 실행 기록에 고정합니다.
- 고정 정책: 슬리피지 0.05%, 수수료 0.2%.

## 저장소 경계

- 주 변경 스키마: `backtest`
- 읽기: `strategy`, `market_data`
- S3: 상세 결과와 대용량 산출물
- PostgreSQL: 실행 상태·요약·S3 객체/Manifest 연결
- DB 접근은 필요한 범위의 SQLAlchemy Core만 사용합니다.
- Alembic은 사용하지 않으며 루트 저장소 Flyway가 마이그레이션을 관리합니다.

## Git Flow

- `develop`이 기본 개발 브랜치입니다.
- `feature/*`, `fix/*`, `docs/*`, `chore/*`는 `develop`에서 시작해 `develop`으로 병합합니다.
- `release/*`로 정식 릴리스를 준비합니다.
- `main`에는 v1.0.0부터 검증된 정식 릴리스만 병합합니다.
- 정식 릴리스 이후 `hotfix/*`는 `main`과 `develop` 모두에 반영합니다.

