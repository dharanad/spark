# Adaptive Query Execution (AQE) — Deep Dive

## 1. What Is AQE?

Traditional Spark query planning is **static**: the optimizer produces a physical plan once before any data is read, relying entirely on estimated statistics (row counts, data sizes). Those estimates are often wrong — stale table statistics, no stats at all, or data skew that no static model can predict. The result is bad join choices, too many shuffle partitions, and straggler tasks.

**Adaptive Query Execution (AQE)** is a framework that re-optimizes the running query mid-flight, each time a shuffle or broadcast stage finishes materializing its output. At that point the actual shuffle map output statistics — real byte counts per partition — are available, and AQE uses them to make better decisions about what remains to execute.

Enabled by default since Spark 3.2:

```
spark.sql.adaptive.enabled = true   (default: true)
```

---

## 2. Core Concepts

### Query Stage

A **query stage** (`QueryStageExec`) is an independent subgraph of the physical plan that must fully materialize before the operators above it can start. AQE carves the plan at every `Exchange` node (shuffle or broadcast) and every `InMemoryTableScan` into stages.

```
         SortMergeJoin
        /             \
  ShuffleStage1    ShuffleStage2
     (sort left)    (sort right)
```

Stage boundaries are the observation points: once Stage1 finishes, we know exactly how much data each of its output partitions holds.

There are three concrete subtypes:

| Subtype | Wraps | Materialization produces |
|---|---|---|
| `ShuffleQueryStageExec` | `ShuffleExchangeLike` | `MapOutputStatistics` (bytes per partition) |
| `BroadcastQueryStageExec` | `BroadcastExchangeLike` | broadcast relation |
| `TableCacheQueryStageExec` | `InMemoryTableScanLike` | cached in-memory data |

After a stage is materialized, `stage.computeStats()` returns a `Statistics` object with `isRuntime = true`, marking it as real data rather than an estimate.

### LogicalQueryStage

Before re-optimizing, AQE patches the logical plan by replacing finished logical subtrees with `LogicalQueryStage` nodes — lightweight leaf nodes that wrap a `QueryStageExec` and delegate `computeStats()` to the actual runtime statistics of that stage.

This is the bridge that lets the AQE Optimizer "see" real statistics when it re-runs logical-plan rules. A `LogicalQueryStage` returns `Statistics(isRuntime = true)` from its materialized physical stage instead of the original estimate.

### AdaptiveSparkPlanExec

`AdaptiveSparkPlanExec` is a `LeafExecNode` that replaces the root of the physical plan when AQE is active. It owns the entire adaptive execution loop.

```scala
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan,
    context: AdaptiveExecutionContext,
    preprocessingRules: Seq[Rule[SparkPlan]],
    isSubquery: Boolean, ...)
  extends LeafExecNode
```

Key fields:
- `initialPlan` — the physical plan after preparation rules, before any adaptive changes
- `currentPhysicalPlan` — the live plan, updated each time a better plan is found
- `optimizer: AQEOptimizer` — used to re-optimize the logical plan between stages
- `costEvaluator: CostEvaluator` — decides whether a newly re-planned plan is cheaper than the current one (default: `SimpleCostEvaluator`)

---

## 3. When Is AQE Invoked?

### Step 1 — `InsertAdaptiveSparkPlan` (preparation phase)

AQE is inserted during the **physical plan preparation** phase, in `QueryExecution`:

```scala
// QueryExecution.scala
protected def preparations: Seq[Rule[SparkPlan]] = {
  QueryExecution.preparations(sparkSession,
    Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))), ...)
}
```

`InsertAdaptiveSparkPlan` is a `Rule[SparkPlan]` that wraps the physical plan in `AdaptiveSparkPlanExec` if AQE is applicable. It short-circuits for plans that don't need AQE:

```scala
case _ if !conf.adaptiveExecutionEnabled => plan          // disabled
case _: ExecutedCommandExec => plan                       // DDL/DML commands
case _ if statefulStreamingPlanExists => plan             // stateful streaming
case _ if shouldApplyAQE(plan, isSubquery) => ...         // wrap in AdaptiveSparkPlanExec
```

`shouldApplyAQE` returns true when the plan contains any of:
- An `Exchange` node (shuffle or broadcast)
- A node with a non-trivial `requiredChildDistribution` (would add an Exchange later)
- A subquery expression
- A cached `InMemoryTableScanExec` that itself was AQE-ed

If none of those are true, the plan cannot benefit from AQE and is returned unchanged.

### Step 2 — First `execute()` call on `AdaptiveSparkPlanExec`

Nothing runs when the plan is constructed. Execution begins only when Spark calls `doExecute()` / `executeCollect()` / `executeTake()` on the `AdaptiveSparkPlanExec` root node, which all delegate to `withFinalPlanUpdate(fun)`.

---

## 4. How AQE Works — The Execution Loop

`withFinalPlanUpdate` runs the adaptive loop:

```
withFinalPlanUpdate(fun):
  1. createQueryStages(currentPhysicalPlan)
     — traverse bottom-up, wrap each ready Exchange in a QueryStageExec
     — if all children of an Exchange are materialized, create a new stage for it
     — otherwise keep the Exchange as-is (not yet ready to stage-ify)

  2. submit all newly created stages for asynchronous materialization
     (broadcast stages are submitted first to avoid timeout)

  3. wait for the next stage completion event (blocking)

  4. re-optimize:
     a. replaceWithQueryStagesInLogicalPlan — patch logical plan with LogicalQueryStage nodes
     b. reOptimize(logicalPlan):
          i.  invalidateStatsCache on the patched logical plan
          ii. AQEOptimizer.execute(logicalPlan)   ← AQE Optimizer runs here
          iii.SparkPlanner.plan(optimized)         ← re-plan from scratch
          iv. apply queryStagePreparationRules     ← EnsureRequirements etc.
     c. compare cost: if newCost <= currentCost, adopt new plan

  5. go back to step 1 with the (possibly updated) currentPhysicalPlan

  6. when all stages are materialized and a ResultQueryStageExec is created,
     execute the final stage and return results
```

### Stage creation (bottom-up traversal)

`createNonResultQueryStages` traverses the plan bottom-up:
- At a leaf node → all children materialized = true (nothing to wait for)
- At an `Exchange` node:
  - if all children are already materialized → **create a new `QueryStageExec`** and kick off materialization asynchronously
  - if any child is still unmaterialized → return the Exchange as-is; revisit next iteration
- At a `QueryStageExec` → check `isMaterialized`
- At any other node → recurse into children, propagate `allChildStagesMaterialized`

### Cost evaluation

After re-optimizing, the new plan's cost is compared to the current plan's cost via `CostEvaluator`. The default `SimpleCostEvaluator` counts the number of `ShuffleExchangeLike` nodes — fewer shuffles is cheaper. If the new plan is not cheaper it is discarded and the current plan continues executing.

### Exchange reuse

When AQE creates a new stage for an Exchange, it checks `context.stageCache` (keyed by `canonicalized` Exchange) before creating a duplicate. If a matching stage already exists, it is reused — the same shuffle data is read twice without re-materializing.

---

## 5. The AQE Optimizer

### What it is

`AQEOptimizer` is a `RuleExecutor[LogicalPlan]` — the same kind of rule executor as the static `Optimizer` — but runs **at runtime**, not at plan time, and operates on a patched logical plan where finished stages appear as `LogicalQueryStage` nodes with real statistics.

```scala
// AQEOptimizer.scala
class AQEOptimizer(conf: SQLConf, extendedRuntimeOptimizerRules: Seq[Rule[LogicalPlan]])
  extends RuleExecutor[LogicalPlan]
```

### When it runs

Called inside `reOptimize()` in `AdaptiveSparkPlanExec`, **once for each stage completion** that triggers a replanning attempt:

```scala
// AdaptiveSparkPlanExec.reOptimize
logicalPlan.invalidateStatsCache()           // force re-read of stats from LogicalQueryStage nodes
val optimized = optimizer.execute(logicalPlan) // AQE Optimizer runs here
val sparkPlan = planner.plan(ReturnAnswer(optimized)).next() // re-plan with real stats
```

The stats cache must be invalidated before every AQE Optimizer run so that the optimizer re-reads the now-updated `LogicalQueryStage.computeStats()` values.

### Rule batches

| Batch | Strategy | Rules |
|---|---|---|
| `Propagate Empty Relations` | FixedPoint | `AQEPropagateEmptyRelation`, `ConvertToLocalRelation`, `UpdateAttributeNullability` |
| `Dynamic Join Selection` | Once | `DynamicJoinSelection` |
| `Eliminate Limits` | FixedPoint | `EliminateLimits` |
| `Optimize One Row Plan` | FixedPoint | `OptimizeOneRowPlan` |
| `User Provided Runtime Optimizers` | FixedPoint | user-supplied rules (see below) |

#### `DynamicJoinSelection` (the most impactful rule)

This is the rule that enables **runtime join strategy switching** — the feature most people associate with AQE.

It inspects `LogicalQueryStage` nodes wrapping `ShuffleQueryStageExec` and reads their `mapStats: MapOutputStatistics`. Based on actual partition byte counts it adds join hints:

| Observation | Hint added | Effect |
|---|---|---|
| A join side has a high ratio of **empty partitions** (> threshold) | `NO_BROADCAST_HASH` on that side | Forces shuffle join instead of broadcast — many tasks finish instantly on the empty side |
| Every partition of a join side is **small** (< `ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD`) | `PREFER_SHUFFLE_HASH` | Encourages shuffled-hash join over sort-merge join |
| Both conditions above apply | `SHUFFLE_HASH` | Combines the above |

After the AQE Optimizer adds these hints, `SparkPlanner` runs again and `JoinSelection` uses the hints to pick a different physical join strategy than it did statically.

This is how AQE upgrades a sort-merge join to a broadcast hash join at runtime when it discovers that one side turned out to be small.

#### `AQEPropagateEmptyRelation`

Runtime version of `PropagateEmptyRelation`. If a materialized stage produced zero rows, the `LogicalQueryStage.computeStats()` reports `rowCount = 0`. This rule propagates that emptiness upward — e.g., an inner join with an empty side can be eliminated.

#### `EliminateLimits` / `OptimizeOneRowPlan`

Logical clean-up rules that use runtime row counts to eliminate redundant limit operators or simplify single-row plans.

### Rule exclusion

Rules in `AQEOptimizer` can be excluded via:

```
spark.sql.adaptive.optimizer.excludedRules = "rule1,rule2"
```

This is separate from `spark.sql.optimizer.excludedRules` (static optimizer).

---

## 6. Physical-Layer Optimization — QueryStage Optimizer Rules

In addition to the logical-layer `AQEOptimizer`, AQE runs two sets of **physical** rules on each new query stage before it executes:

### `queryStageOptimizerRules` — per-stage physical rules

Applied to every new stage (after `AQEOptimizer` and re-planning):

| Rule | What it does |
|---|---|
| `PlanAdaptiveDynamicPruningFilters` | Wires up dynamic partition pruning filters that reference broadcast stages now available |
| `ReuseAdaptiveSubquery` | Reuses already-materialized subquery stages |
| `OptimizeSkewInRebalancePartitions` | Splits skewed partitions in rebalance operations |
| `CoalesceShufflePartitions` | Merges small shuffle partitions to reduce task count |
| `OptimizeShuffleWithLocalRead` | When possible, converts a shuffle read to a local (co-located) read, eliminating network transfer |

### `queryStagePreparationRules` — stage preparation (run once per re-plan)

Applied before creating stages, on the freshly re-planned physical tree:

| Rule | What it does |
|---|---|
| `CoalesceBucketsInJoin` | Coalesces bucket counts to eliminate unnecessary shuffles |
| `EnsureRequirements` | Adds missing exchanges and sorts for distribution/ordering requirements |
| `AdjustShuffleExchangePosition` | Moves exchanges to optimal positions |
| `ReplaceHashWithSortAgg` | Replaces hash aggregation with sort aggregation when beneficial |
| `RemoveRedundantSorts` | Eliminates sorts that are redundant given current ordering |
| `OptimizeSkewedJoin` | Splits skewed partitions in sort-merge joins into sub-partitions |
| `DisableUnnecessaryBucketedScan` | Disables bucket scan when the query doesn't need it |

### `postStageCreationRules` — applied right after each new stage is created

```
ApplyColumnarRulesAndInsertTransitions   — insert columnar → row / row → columnar transitions
CollapseCodegenStages                    — group adjacent codegen-capable nodes into WholeStageCodegen
```

---

## 7. Re-Optimization Flow — Full Picture

```
A shuffle stage finishes materializing
          |
          v
replaceWithQueryStagesInLogicalPlan
  — logical plan nodes for this stage replaced with LogicalQueryStage(runtime stats)
          |
          v
reOptimize(patchedLogicalPlan):
  |
  +-- invalidateStatsCache()                   force re-read stats from LogicalQueryStage nodes
  |
  +-- AQEOptimizer.execute(logicalPlan)        logical re-optimization with real stats
  |     batch: Propagate Empty Relations       eliminate empty-relation joins/unions
  |     batch: Dynamic Join Selection          add hints based on actual partition sizes
  |     batch: Eliminate Limits
  |     batch: Optimize One Row Plan
  |     batch: User Provided Runtime Optimizers
  |
  +-- SparkPlanner.plan(optimized)             physical re-planning using updated hints
  |     JoinSelection reads NO_BROADCAST_HASH / PREFER_SHUFFLE_HASH hints
  |     → may switch SortMergeJoin to BroadcastHashJoin, etc.
  |
  +-- applyQueryPostPlannerStrategyRules       user extension point (before EnsureRequirements)
  |
  +-- queryStagePreparationRules               EnsureRequirements, OptimizeSkewedJoin, etc.
  |
  +-- CostEvaluator.compare(current, new)
        if newCost <= currentCost → adopt newPlan
        else → discard, keep currentPlan
```

---

## 8. Extension Points

Custom rules can be injected via `AdaptiveRulesHolder` (registered through `SparkSessionExtensions`):

| Hook | Type | When applied |
|---|---|---|
| `queryStagePrepRules` | `Seq[Rule[SparkPlan]]` | Appended to `queryStagePreparationRules` (before stage creation) |
| `runtimeOptimizerRules` | `Seq[Rule[LogicalPlan]]` | Appended as `"User Provided Runtime Optimizers"` batch in `AQEOptimizer` |
| `queryStageOptimizerRules` | `Seq[Rule[SparkPlan]]` | Appended to `queryStageOptimizerRules` (per new stage) |
| `queryPostPlannerStrategyRules` | `Seq[Rule[SparkPlan]]` | Applied between SparkPlanner and queryStagePreparationRules |

Register via:
```scala
spark.extensions { ext =>
  ext.injectQueryStagePrepRule(_ => MyQueryStagePrepRule)
  ext.injectRuntimeOptimizerRule(_ => MyRuntimeLogicalRule)
  ext.injectQueryStageOptimizerRule(_ => MyStageOptimizerRule)
  ext.injectQueryPostPlannerStrategyRule(_ => MyPostPlannerRule)
}
```

---

## 9. Key Configuration

| Config | Default | Description |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | Master on/off switch |
| `spark.sql.adaptive.forceApply` | `false` | Apply AQE even when the plan has no exchanges (e.g. for testing) |
| `spark.sql.adaptive.logLevel` | `debug` | Log level for AQE plan changes |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Enable `CoalesceShufflePartitions` |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | Maximize parallelism when coalescing |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target partition size after coalescing |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | Floor for coalesced partition size |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Enable `OptimizeSkewedJoin` |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | Partition larger than this is skewed |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5.0` | Partition is skewed if size > factor * median |
| `spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin` | `0.2` | Ratio below which DynamicJoinSelection adds NO_BROADCAST_HASH |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0B` | Max partition size for PREFER_SHUFFLE_HASH |
| `spark.sql.adaptive.optimizer.excludedRules` | (empty) | Comma-separated list of AQE Optimizer rules to disable |
| `spark.sql.adaptive.customCostEvaluatorClass` | (empty) | Fully-qualified class name of a custom `CostEvaluator` |
| `spark.sql.adaptive.applyFinalStageShuffleOptimizations` | `true` | Apply `AQEShuffleReadRule`s to the final stage |

---

## 10. Key Source Files

| File | Role |
|---|---|
| `execution/adaptive/InsertAdaptiveSparkPlan.scala` | Rule that wraps plan in AQE; entry point for deciding whether AQE applies |
| `execution/adaptive/AdaptiveSparkPlanExec.scala` | Core adaptive execution engine; owns the event loop and re-planning logic |
| `execution/adaptive/AQEOptimizer.scala` | `RuleExecutor[LogicalPlan]` run at runtime between stages |
| `execution/adaptive/QueryStageExec.scala` | Base class for query stages; `ShuffleQueryStageExec`, `BroadcastQueryStageExec`, `TableCacheQueryStageExec` |
| `execution/adaptive/LogicalQueryStage.scala` | Logical plan wrapper for a materialized stage; provides runtime statistics to AQE Optimizer |
| `execution/adaptive/DynamicJoinSelection.scala` | AQE Optimizer rule that switches join strategy at runtime |
| `execution/adaptive/CoalesceShufflePartitions.scala` | Physical rule that merges small shuffle partitions |
| `execution/adaptive/OptimizeSkewedJoin.scala` | Physical rule that splits skewed sort-merge join partitions |
| `execution/adaptive/OptimizeShuffleWithLocalRead.scala` | Converts remote shuffle reads to local reads |
| `execution/adaptive/AQEPropagateEmptyRelation.scala` | Eliminates joins/unions when a stage produced zero rows |
| `execution/adaptive/AdaptiveRulesHolder.scala` | Container for user-injected extension rules |
| `execution/adaptive/costing.scala` / `simpleCosting.scala` | `CostEvaluator` interface and default implementation |
