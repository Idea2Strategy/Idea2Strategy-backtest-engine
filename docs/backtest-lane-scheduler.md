# Backtest lane scheduler

The worker can share one compute host across three independent SQS queues. Its
default admission budget is:

| Lane | Concurrent executions |
| --- | ---: |
| `basic` | 2 |
| `custom` | 1 |
| `competition` | 1 |
| Total | 4 |

The scheduler reserves both a lane slot and a global slot before receiving a
message. Requests above either limit remain visible in SQS. Eligible queues are
polled round-robin, so a continuously busy `basic` queue cannot starve the
other lanes.

The upstream publisher remains responsible for classifying a request and
sending it to the correct queue. The backtest worker does not infer or change
that product meaning.

## Required environment

Set the existing `BACKTEST_WORKER_ID`, `BACKTEST_JOB_HANDLER`, and
`BACKTEST_EXECUTION_KEY_STORE` values, plus both URLs for each lane:

```text
BACKTEST_BASIC_QUEUE_URL
BACKTEST_BASIC_DLQ_URL
BACKTEST_CUSTOM_QUEUE_URL
BACKTEST_CUSTOM_DLQ_URL
BACKTEST_COMPETITION_QUEUE_URL
BACKTEST_COMPETITION_DLQ_URL
```

The optional concurrency settings default to the table above:

```text
BACKTEST_BASIC_MAX_CONCURRENCY=2
BACKTEST_CUSTOM_MAX_CONCURRENCY=1
BACKTEST_COMPETITION_MAX_CONCURRENCY=1
BACKTEST_MAX_TOTAL_CONCURRENCY=4
BACKTEST_SCHEDULER_IDLE_SECONDS=5
```

The existing visibility timeout, heartbeat interval, and maximum receive count
settings apply to every lane. A handler success is acknowledged only after the
execution-key store records terminal success. Retryable failures release the
claim and return the message to its source queue; permanent failures use that
lane's dead-letter queue.
