# Cross-runtime conformance fixtures (card D92)

`strategy-bot-runtime/v1/basic-executor-conformance.v1.json` is **one set of bytes
consumed by two languages**. The Python backtest runtime in this repository and the
Java trading runtime in `trading-engine` are supposed to make the same decisions from
the same plan and the same market events; this directory is what turns that from a
claim into a test result.

Nothing in `src/` reads these files. They are test inputs on both sides, and
`tests/test_d92_runtime_conformance.py::test_the_fixture_is_read_as_bytes_not_regenerated`
enforces that, so the fixture can never become an output of the implementation it is
supposed to check.

## Why here and not in `contracts/`

The fixture lives with its Python consumer so the package can run independently. The
root `contracts/` tree remains the source of cross-service obligations; update it and
both language bindings together whenever the fixture changes product behavior.

## Integrity

`basic-executor-conformance.v1.json.sha256` records the digest of the fixture bytes.
Both language bindings assert it. A fixture edited on one side without the other
noticing therefore fails on **both** sides rather than diverging quietly:

```
2e7e29c9f6dc90ed53c1edf75887d9e4ee209af02db88781bd22421914fb74d5  basic-executor-conformance.v1.json
```

Regenerate with `sha256sum basic-executor-conformance.v1.json` after any edit, and
change the digest and the fixture in the same commit.

## Structure

| section | what it pins | which layer it checks |
|---|---|---|
| `features` | the RSI-14 definition: method, periodicity, warm-up, formula, arithmetic mode, rounding | the feature implementation |
| `featureVectors` | six `(closes) -> value` literals, each with a written derivation | the feature arithmetic |
| `elementCases` | `LOAD_FEATURE` / `COMPARE` outcomes, including warm-up incomplete, series missing, look-ahead exclusion and numeric-not-textual comparison | the element layer |
| `executorCases` | ordering, first-failure short-circuit, the four decision statuses, and exact 1/N allocation | the executor itself |
| `ordering` | flow order, instrument order and the instrument comparison rule | both |
| `knownDivergences` | places where the two runtimes really do differ today | both |

`executorCases` deliberately supply each step's **outcome** rather than market data.
That is what makes them language-neutral: `BasicStrategyExecutor` takes its condition
steps as `Function<BasicInstrumentInput, BasicConditionOutcome>` and has no feature
implementation of its own, so the executor's own responsibilities — ordering,
short-circuit, status mapping, allocation — are exactly what a scripted outcome
isolates. The element and feature layers are pinned separately, so between the three
sections nothing is left unchecked.

---

# What the trading-engine test must do

This section is the specification for a test **in the `trading-engine` repository**.
Nothing in `trading-engine` has been edited by this change; D cannot and did not.

Suggested location:
`trading-engine/modules/strategy-runtime/src/test/java/com/idea2strategy/trading/strategy/runtime/basic/BasicExecutorConformanceTest.java`

## 1. Read the identical bytes

Resolve the fixture from the sibling submodule checkout, allow an override, and fail —
do not skip — when it cannot be found in CI:

```java
Path fixture = Optional.ofNullable(System.getenv("IDEA2STRATEGY_RUNTIME_CONFORMANCE"))
        .map(Path::of)
        .orElseGet(() -> Path.of("..", "..", "backtest-engine", "conformance",
                "strategy-bot-runtime", "v1", "basic-executor-conformance.v1.json"));
```

Assert the SHA-256 of the bytes equals the digest in the `.sha256` file next to it
before parsing anything. Reading a different file than Python read is the one failure
mode this whole exercise cannot tolerate.

## 2. Assert the versions the fixture declares

```
runtimeSchemaVersion  == StrategyBotExecutionPlanAdapter.RUNTIME_SCHEMA_VERSION  // "strategy-bot-runtime.v1"
reasonCodes.conditionError == "CONDITION_EVALUATION_ERROR"                        // BasicStrategyExecutor.CONDITION_ERROR
reasonCodes.inputMissing   == "INSTRUMENT_INPUT_MISSING"                          // BasicStrategyExecutor.INPUT_MISSING
reasonCodes.missingInputStepId == "$input"
```

A fixture written against a different runtime schema version must fail the test, not
be interpreted leniently — the same reason `StrategyBotExecutionPlanAdapter` has a
`RUNTIME_SCHEMA_VERSION_MISMATCH` failure at all.

## 3. Drive `executorCases` through the real `BasicStrategyExecutor`

For each case:

1. Build one `BasicFlow` per entry in `flows`:
   - `flowId` from `flowId`, `side` from `side` (`BasicOrderSide.valueOf`).
   - `instrumentIds` **in the fixture's declared order**. Do not pre-sort them; the
     executor's own ordering is under test.
   - one `BasicConditionStep` per entry in `conditionSteps`, whose `stepId` is the
     fixture string verbatim (`"step-1:LOAD_FEATURE"` and so on) and whose evaluator
     is scripted from `stepOutcomes[instrumentId]`, advancing one entry per call
     **per flow** (the same instrument can appear in two flows and must replay the
     script from the start in each):

   | `outcome` | the evaluator must |
   |---|---|
   | `PASSED` | return `new BasicConditionOutcome(true, reasonCode, evidence)` |
   | `FAILED` | return `new BasicConditionOutcome(false, reasonCode, evidence)` |
   | `RAISES_EVALUATION_ERROR` | throw a `RuntimeException` |
   | `MUST_NOT_BE_EVALUATED` | **fail the test immediately** |

   `MUST_NOT_BE_EVALUATED` is not decoration. Asserting only on trace length would
   let an implementation evaluate a step after the short circuit and then discard the
   result, which for an evaluator with side effects is a different program.

2. Build `BasicExecutionRequest` with one `BasicInstrumentInput` per instrument,
   **omitting** any instrument listed in `instrumentsWithoutInput`. The map key must
   equal the input's own `instrumentId`.

3. Call `new BasicStrategyExecutor().execute(request)` and compare
   `result.decisions()` with `expectedDecisions` **positionally**, in order:
   `flowId`, `instrumentId`, `side`, `status`, `firstFailureStepId`,
   `firstFailureReason`, `buyAllocation`, and the whole `trace` (step ids, `passed`,
   `reasonCode`, evidence).

   - `buyAllocation` of `null` means `Optional.empty()`. Otherwise it means
     `Optional.of(new EqualAllocationShare(numerator, denominator))` — compare the
     record, never a converted decimal.
   - An evidence value of `"$IMPLEMENTATION_DEFINED"` means: the key must be present
     and non-empty, and its value is not compared.
   - When a trace entry sets `additionalEvidenceKeysPermitted: true`, extra keys are
     allowed; otherwise the evidence maps must be equal.

4. When the case has `expectedEvaluationOrder`, record the order in which the scripted
   evaluators were first called per instrument and assert it equals that list.

## 4. Expect `instrument-iteration-order-discriminates-signed-from-unsigned` to fail today

That case exists to make a real divergence visible, and the Java executor currently
sorts with `Comparator.naturalOrder()`. `java.util.UUID.compareTo` compares
`mostSigBits` and `leastSigBits` as **signed** longs, so an id beginning `0x80`–`0xff`
sorts before every id beginning `0x00`–`0x7f`. Python's `uuid.UUID` ordering, and the
fixture's normative `UNSIGNED_128BIT_BIG_ENDIAN` rule, do the opposite.

**Do not make this pass by editing the fixture.** Two honest options:

- adopt the unsigned rule in `BasicStrategyExecutor` by replacing
  `Comparator.naturalOrder()` with an explicit comparator, for example

  ```java
  private static final Comparator<UUID> UNSIGNED_BIG_ENDIAN =
          Comparator.comparing(id -> id.toString());   // canonical text == unsigned byte order
  ```

  (or compare `mostSigBits`/`leastSigBits` with `Long.compareUnsigned`);
- or reject the proposal and record the decision, in which case D changes instead and
  the fixture's normative rule changes with it.

The unsigned rule is what this repository proposes, because it is the order the
canonical text form of a UUID already gives and the order PostgreSQL's `uuid` type
already uses (`uuid_cmp` is a `memcmp`), so every other layer of the system agrees
with it. It is a **proposal**: D cannot decide C's runtime semantics.

## 5. The feature layer, when the Java runtime grows one

`featureVectors` and `elementCases` are directly usable as Java test vectors the day
`trading-engine` implements `LOAD_FEATURE`. Until then they are the written
specification the implementation must be built to:

- `new MathContext(34, RoundingMode.HALF_EVEN)` for the working arithmetic;
- `value.setScale(8, RoundingMode.HALF_EVEN)` once, at the end;
- `BigDecimal` throughout — `double` does not round-trip the 8-decimal contract value;
- the simple-average ("Cutler's") RSI, **not** Wilder's smoothing;
- 15 completed bars, and warm-up short is an input-missing outcome, never a
  substituted 0, 50 or 100;
- the `U == 0 && V == 0 -> 50` branch reproduced verbatim.

## 6. Report back rather than diverge

If the Java side finds a case it believes is wrong, the fixture is the thing to argue
with, not the thing to edit unilaterally: it is shared bytes with a recorded digest,
and changing it on one side breaks the other by construction. That is the property
that makes this a conformance suite instead of two test suites that happen to be
similar.
