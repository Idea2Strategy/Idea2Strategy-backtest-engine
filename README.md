# Idea2Strategy Backtest Engine

출시된 잠긴 전략을 검증된 시장 데이터 스냅샷으로 자동 백테스트하는 Python 저장소입니다.

## 책임

- FastAPI 기반 내부 작업 접수·상태·진단 API
- 별도 Worker의 CPU 집약 백테스트
- Parquet 입력과 Polars·PyArrow 연산
- 주문·개별 체결·취소·거절·비용·성과 계산
- 미국 동부 시각 월별 판단 요약과 거래 상세 결과 생성

HTTP 요청 프로세스의 `BackgroundTasks`에서 백테스트 계산을 실행하지 않습니다. API는 작업을 등록하고 Worker가 Queue를 통해 수행합니다.

```text
apps/api/
workers/
src/backtest_engine/
  strategy_runtime/
  simulation/
  order_model/
  portfolio/
  performance/
  market_data/
  manifests/
  persistence/
tests/
```

자세한 경계는 [DEVELOPMENT.md](DEVELOPMENT.md)를 확인합니다.

