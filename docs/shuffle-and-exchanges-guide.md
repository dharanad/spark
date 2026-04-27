# Spark Shuffle and Exchanges: A Deep Dive

This guide covers how data movement works in Apache Spark — from the abstract concept of an Exchange
operator to the physical shuffle files written to disk. It is developer-focused and traces the full
path from logical planning through physical execution, including the algorithms that drive each step.

---

## Table of Contents

1. [What Is a Shuffle?](#1-what-is-a-shuffle)
2. [The Exchange Operator Hierarchy](#2-the-exchange-operator-hierarchy)
3. [ShuffleOrigin — Why a Shuffle Was Inserted](#3-shuffleorigin--why-a-shuffle-was-inserted)
4. [Distribution and Partitioning](#4-distribution-and-partitioning)
5. [EnsureRequirements — The Rule That Inserts Exchanges](#5-ensurerequirements--the-rule-that-inserts-exchanges)
6. [ShuffleExchangeExec — Physical Execution In Detail](#6-shuffleexchangeexec--physical-execution-in-detail)
7. [BroadcastExchangeExec — Broadcast Joins](#7-broadcastexchangeexec--broadcast-joins)
8. [ReusedExchangeExec — Exchange Reuse](#8-reusedexchangeexec--exchange-reuse)
9. [Core Shuffle: ShuffleDependency](#9-core-shuffle-shuffledependency)
10. [SortShuffleManager and the Three Write Paths](#10-sortshufflemanager-and-the-three-write-paths)
11. [IndexShuffleBlockResolver — Map Output Storage](#11-indexshuffleblockresolver--map-output-storage)
12. [ShuffledRowRDD and ShufflePartitionSpec](#12-shuffledrowrdd-and-shufflepartitionspec)
13. [MapOutputStatistics and AQE Integration](#13-mapoutputstatistics-and-aqe-integration)
14. [Key Configuration Properties](#14-key-configuration-properties)
15. [Key Source Files](#15-key-source-files)

---

## 1. What Is a Shuffle?

A **shuffle** is the process of redistributing data across partitions between two stages of a Spark job.
It is the most expensive operation in distributed query processing because it requires every mapper to
write output files to local disk and every reducer to fetch data over the network from potentially
every mapper.

Shuffles are needed whenever the output partitioning of a child operator does not satisfy the
partitioning requirement of the parent operator. Common cases:

- A `JOIN` requires both sides to have the same key hashed to the same partition.
- An `AGGREGATE` requires all rows with the same group-by key to be in the same partition.
- A `SORT` with a global ordering requires a range-partitioned layout.
- An explicit `REPARTITION` by the user.

### The Two-Phase Model

A shuffle always has two phases separated by a stage boundary:

**Map phase** (upstream stage):
1. Each mapper executes the child plan producing `InternalRow` objects.
2. For each row, a **partition key** is computed — typically a hash of the join/group-by columns.
3. Rows are written to a local shuffle **data file** with an accompanying **index file**.
4. When all mappers complete, the driver reads `MapOutputStatistics` from `MapOutputTracker`.

**Reduce phase** (downstream stage):
1. Each reducer task knows which `shuffleId` and `reducePartitionId` it owns.
2. It queries `MapOutputTracker` to find where each mapper wrote the bytes for that partition.
3. `ShuffleBlockFetcherIterator` fetches remote blocks over the network and reads local blocks
   from disk directly.
4. The reducer iterates over the fetched rows and feeds them to the consuming operator.

In Spark SQL, the map phase corresponds to executing `ShuffleExchangeExec.inputRDD` and the reduce
phase corresponds to reading from `ShuffledRowRDD`.

---

## 2. The Exchange Operator Hierarchy

```
SparkPlan
└── UnaryExecNode
    └── Exchange (abstract)
        ├── ShuffleExchangeLike (trait)  ←─  ShuffleExchangeExec
        └── BroadcastExchangeLike (trait) ←─ BroadcastExchangeExec

SparkPlan
└── LeafExecNode
    └── ReusedExchangeExec  (wraps a previously-computed Exchange)
```

### `Exchange` (abstract base)

`Exchange` extends `UnaryExecNode`. Its `output` is simply `child.output` — the exchange does not
change the schema, only where the data lives. All `Exchange` nodes share the `EXCHANGE`
`TreePattern`, which enables efficient pattern-based pruning during plan traversal.

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/Exchange.scala
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  final override val nodePatterns: Seq[TreePattern] = Seq(EXCHANGE)
}
```

The `EXCHANGE` tree pattern lets rules use `containsPattern(EXCHANGE)` to skip subtrees that
have no exchange nodes, avoiding unnecessary traversal cost.

### `ShuffleExchangeLike` trait

Adds the shuffle-specific API that AQE needs to submit and introspect shuffle jobs:

```scala
trait ShuffleExchangeLike extends Exchange {
  def shuffleId: Int
  def numMappers: Int
  def numPartitions: Int
  def advisoryPartitionSize: Option[Long]
  def shuffleOrigin: ShuffleOrigin
  def mapOutputStatisticsFuture: Future[MapOutputStatistics]
  def submitShuffleJob(): Future[MapOutputStatistics]
  def cancelShuffleJob(reason: Option[String]): Unit
  def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_]
  def runtimeStatistics: Statistics
}
```

The `submitShuffleJob()` method is the key integration point for AQE. It:
1. Triggers an async thread (`ShuffleExchangeExec.executionContext`) to call `executeQuery(null)`,
   which handles subquery preparation and file listing.
2. Once preparation is done, submits the shuffle map stage via `sparkContext.submitMapStage(dep)`.
3. Returns a `Future[MapOutputStatistics]` that AQE awaits before re-optimizing the plan.

### `BroadcastExchangeLike` trait

Adds broadcast-specific API:

```scala
trait BroadcastExchangeLike extends Exchange {
  val runId: UUID = UUID.randomUUID
  def jobTag: String  // used to cancel the broadcast job by tag
  def relationFuture: Future[broadcast.Broadcast[Any]]
  def submitBroadcastJob(): scala.concurrent.Future[broadcast.Broadcast[Any]]
  def cancelBroadcastJob(reason: Option[String]): Unit
  def runtimeStatistics: Statistics
}
```

The broadcast relation is prepared asynchronously in `BroadcastExchangeExec.executionContext` so
the driver can continue planning while the relation is being collected and broadcast.

---

## 3. ShuffleOrigin — Why a Shuffle Was Inserted

`ShuffleOrigin` is a sealed trait that records the reason a `ShuffleExchangeExec` was inserted.
This matters for AQE (which rules can apply) and for optimizer decisions about reuse.

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala
sealed trait ShuffleOrigin

case object ENSURE_REQUIREMENTS extends ShuffleOrigin
case object REPARTITION_BY_COL extends ShuffleOrigin
case object REPARTITION_BY_NUM extends ShuffleOrigin
case object REBALANCE_PARTITIONS_BY_NONE extends ShuffleOrigin
case object REBALANCE_PARTITIONS_BY_COL extends ShuffleOrigin
case object REQUIRED_BY_STATEFUL_OPERATOR extends ShuffleOrigin
```

| Value | Source | AQE coalesce? |
|---|---|---|
| `ENSURE_REQUIREMENTS` | Inserted by `EnsureRequirements` to satisfy operator requirements | Yes |
| `REPARTITION_BY_COL` | User called `df.repartition(cols*)` | Yes (partition count only) |
| `REPARTITION_BY_NUM` | User called `df.repartition(n)` | No — user fixed the count |
| `REBALANCE_PARTITIONS_BY_NONE` | `REBALANCE` hint, no columns | Yes, with local read preferred |
| `REBALANCE_PARTITIONS_BY_COL` | `REBALANCE` hint with columns | Yes, but no local read |
| `REQUIRED_BY_STATEFUL_OPERATOR` | Stateful streaming operator (e.g., stream-stream join) | No — partitioning is immutable |

The `EnsureRequirements` rule also **optimizes out** redundant user-specified repartitions:

```scala
// EnsureRequirements.apply — strip out REPARTITION_BY_COL/NUM if the child already satisfies
case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin, _)
    if optimizeOutRepartition &&
      (shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM) =>
  if (hasSemanticEqualPartitioning(child.outputPartitioning)) child else operator
```

---

## 4. Distribution and Partitioning

The Distribution/Partitioning system is the contract between operators about how data must be
arranged. Understanding it deeply is essential for understanding when and why exchanges are inserted.

### 4.1 Distributions — What an Operator Requires

`Distribution` is a sealed trait. Each physical operator declares what it needs via
`requiredChildDistribution: Seq[Distribution]`.

```scala
sealed trait Distribution {
  def requiredNumPartitions: Option[Int]
  def createPartitioning(numPartitions: Int): Partitioning
}
```

The full hierarchy:

```
Distribution
├── UnspecifiedDistribution          — no requirement (default)
│     requiredNumPartitions = None
│     createPartitioning → throws (never called)
│
├── AllTuples                        — all rows in ONE partition
│     requiredNumPartitions = Some(1)
│     createPartitioning → SinglePartition
│
├── ClusteredDistribution(keys)      — rows with same key hash in same partition
│     requiredNumPartitions = None (or Some(n) for stateful ops)
│     createPartitioning → HashPartitioning(keys, n)
│
├── StatefulOpClusteredDistribution  — like ClusteredDistribution but MUST match
│     (keys, requiredN)               the operator's exact partition count
│     requiredNumPartitions = Some(requiredN)
│     createPartitioning → HashPartitioning(keys, requiredN)
│
├── OrderedDistribution(ordering)    — rows globally range-sorted across partitions
│     requiredNumPartitions = None
│     createPartitioning → RangePartitioning(ordering, n)
│
└── BroadcastDistribution(mode)      — child must be a broadcast relation
      requiredNumPartitions = Some(1)
      createPartitioning → BroadcastPartitioning(mode)
```

#### `ClusteredDistribution` in detail

`ClusteredDistribution` has a flag `requireAllClusterKeys` (default controlled by
`spark.sql.requireAllClusterKeys`, typically `false`). When `false`, a `HashPartitioning` can
satisfy the distribution if its hash expressions are a **subset** of the clustering keys.
When `true`, the expressions must match exactly and in the same order. The strict mode exists
because some operators (e.g., `Aggregate`) rely on the subset relationship for correctness.

### 4.2 Partitionings — What an Operator Produces

`Partitioning` describes how the output is physically distributed:

```scala
trait Partitioning {
  val numPartitions: Int

  // Two-step check: first confirm partition count, then call satisfies0
  final def satisfies(required: Distribution): Boolean =
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)

  // Default: satisfies UnspecifiedDistribution always; AllTuples only if numPartitions == 1
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case AllTuples => numPartitions == 1
    case _ => false
  }
}
```

The full hierarchy:

```
Partitioning
├── UnknownPartitioning(n)              — unknown; never satisfies ClusteredDistribution
├── RoundRobinPartitioning(n)           — round-robin; never satisfies ClusteredDistribution
├── SinglePartition                     — satisfies everything except BroadcastDistribution
├── HashPartitioning(exprs, n)          — hash by exprs; satisfies ClusteredDistribution
│     extends HashPartitioningLike (which is also an Expression)
├── CoalescedHashPartitioning(...)      — AQE-coalesced version of HashPartitioning
├── RangePartitioning(ordering, n)      — range-partitioned; satisfies OrderedDistribution
├── PartitioningCollection(seq)         — satisfies if ANY member satisfies
└── KeyedPartitioning(exprs, n, keys)   — storage-partitioned join (V2 data source)
    └── CoalescedHashPartitioning       — like above but coalesced by AQE
```

#### The `satisfies()` Decision Tree

The critical method in the system. Here is how each important partitioning handles it:

**`SinglePartition.satisfies0`:**
```scala
override def satisfies0(required: Distribution): Boolean = required match {
  case _: BroadcastDistribution => false  // can't broadcast a single partition
  case _ => true                          // satisfies everything else
}
```
One partition trivially co-locates all data together, so aggregates, joins, and sorts are all fine.

**`HashPartitioning.satisfies0` (via `HashPartitioningLike`):**
```scala
override def satisfies0(required: Distribution): Boolean = {
  super.satisfies0(required) || {
    required match {
      case h: StatefulOpClusteredDistribution =>
        // Exact match required — same expressions, same count
        expressions.length == h.expressions.length &&
          expressions.zip(h.expressions).forall { case (l, r) => l.semanticEquals(r) }
      case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
        if (requireAllClusterKeys) {
          c.areAllClusterKeysMatched(expressions) // exact match
        } else {
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        }
      case _ => false
    }
  }
}
```
Key insight: `HashPartitioning(Seq(a, b), 5)` satisfies `ClusteredDistribution(Seq(a, b, c))`
because every row that has the same `(a, b)` hash will end up in the same partition even though
`c` is not part of the hash. The reverse is not true.

**`RangePartitioning.satisfies0`:**
```scala
override def satisfies0(required: Distribution): Boolean = {
  super.satisfies0(required) || {
    required match {
      case OrderedDistribution(requiredOrdering) =>
        val minSize = scala.math.min(requiredOrdering.size, ordering.size)
        requiredOrdering.take(minSize).zip(ordering).forall {
          case (requiredOrder, givenOrder) => requiredOrder.satisfies(givenOrder)
        }
      case ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
        if (requireAllClusterKeys) {
          ordering.length == requiredClustering.length &&
            ordering.zip(requiredClustering).forall { case (o, c) => o.child.semanticEquals(c) }
        } else {
          ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
        }
      case _ => false
    }
  }
}
```

**`PartitioningCollection.satisfies0`:**
```scala
override def satisfies0(required: Distribution): Boolean =
  partitionings.exists(_.satisfies0(required))
```
A collection satisfies a distribution if **any one** of its member partitionings satisfies it.

### 4.3 `HashPartitioning.partitionIdExpression`

The partition ID for a row under `HashPartitioning` is not computed inside the `Partitioner` at
the core level. Instead, it is pre-computed in SQL before the shuffle using:

```scala
// sql/catalyst/.../plans/physical/partitioning.scala
def partitionIdExpression: Expression =
  Pmod(new CollationAwareMurmur3Hash(expressions), Literal(numPartitions))
```

`CollationAwareMurmur3Hash` applies Murmur3 hashing to each expression value, XOR-combining them.
`Pmod` (positive modulo) guarantees a non-negative result in `[0, numPartitions)`.

This pre-computation is done via `UnsafeProjection` in `prepareShuffleDependency`, producing a
row where the first field is already the partition ID. The `PartitionIdPassthrough` partitioner
then simply reads `row.getInt(0)` — no further hashing is done at the core shuffle level.

### 4.4 The `ShuffleSpec` System for Co-Partitioning Decisions

When `EnsureRequirements` encounters a join with two children both requiring `ClusteredDistribution`,
it needs to decide whether they are already co-partitioned, and if not, which side to re-shuffle.
This decision is made via `ShuffleSpec`:

```
ShuffleSpec
├── SinglePartitionShuffleSpec         — child has SinglePartition
├── ShuffleSpecCollection(seq)         — PartitioningCollection
├── HashShuffleSpec(partitioning, dist)— HashPartitioning-based spec
├── CoalescedHashShuffleSpec(...)      — AQE-coalesced hash spec
├── KeyedShuffleSpec(...)              — storage-partitioned join (V2)
└── RangeShuffleSpec(n, dist)          — RangePartitioning-based spec
```

Key methods on `ShuffleSpec`:
- `isCompatibleWith(other)` — returns true if two specs are co-partitioned (no shuffle needed)
- `canCreatePartitioning` — returns true if this spec can be used to repartition the other side
- `createPartitioning(clustering)` — creates a `HashPartitioning` for the other side to use
- `numPartitions` — the number of partitions

The algorithm in `EnsureRequirements` uses these to avoid inserting unnecessary shuffles:
```
For each child that needs ClusteredDistribution:
  spec = child.outputPartitioning.createShuffleSpec(distribution)

bestSpec = pick spec with max numPartitions that canCreatePartitioning

For each child:
  if bestSpec.isCompatibleWith(childSpec) → no shuffle needed
  else → ShuffleExchangeExec(bestSpec.createPartitioning(clustering), child)
```

---

## 5. EnsureRequirements — The Rule That Inserts Exchanges

`EnsureRequirements` is a `Rule[SparkPlan]` applied in the `preparations` phase after physical
planning. It traverses the plan bottom-up via `transformUp` and ensures that every operator's
children satisfy its distribution and ordering requirements.

```
sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala
```

### 5.1 Top-Level `apply` Method

```scala
def apply(plan: SparkPlan): SparkPlan = {
  val newPlan = plan.transformUp {
    // Optimization: strip user-specified repartition if child already has the right partitioning
    case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin, _)
        if optimizeOutRepartition && (shuffleOrigin == REPARTITION_BY_COL
            || shuffleOrigin == REPARTITION_BY_NUM) =>
      if (hasSemanticEqualPartitioning(child.outputPartitioning)) child else operator

    // General case: for every operator, reorder join keys and ensure distribution/ordering
    case operator: SparkPlan =>
      val reordered = reorderJoinPredicates(operator)
      val newChildren = ensureDistributionAndOrdering(
        Some(reordered),
        reordered.children,
        reordered.requiredChildDistribution,
        reordered.requiredChildOrdering,
        ENSURE_REQUIREMENTS)
      reordered.withNewChildren(newChildren)
  }
  // Optionally, also ensure root distribution (used by AQE)
  if (requiredDistribution.isDefined) { ... }
  newPlan
}
```

### 5.2 `ensureDistributionAndOrdering` — The Core Algorithm

This is the method that actually inserts exchanges and sorts. It runs in three passes:

**Pass 1 — Per-child distribution check:**
```
For each child i:
  Split child.outputPartitioning into: non-keyed, grouped keyed, non-grouped keyed
  If non-keyed already satisfies requiredDistribution(i):
    → keep child as-is
  Else if a KeyedPartitioning satisfies (storage-partitioned join):
    → possibly wrap in GroupPartitionsExec (to enforce grouping)
  Else:
    numPartitions = requiredDistribution(i).requiredNumPartitions
                       .getOrElse(conf.numShufflePartitions)
    requiredDistribution(i) match:
      case BroadcastDistribution(mode) → BroadcastExchangeExec(mode, child)
      case _: StatefulOpClusteredDistribution →
        ShuffleExchangeExec(distribution.createPartitioning(n), child,
                            REQUIRED_BY_STATEFUL_OPERATOR)
      case _ →
        ShuffleExchangeExec(distribution.createPartitioning(n), child, shuffleOrigin)
```

**Pass 2 — Co-partitioning optimization (for joins with multiple ClusteredDistribution children):**

This avoids the naïve case where both sides independently choose incompatible partitionings
and both end up being shuffled:

```
childrenIndexes = indices of children requiring ClusteredDistribution

For each such child i:
  spec(i) = children(i).outputPartitioning.createShuffleSpec(distribution(i))

shouldConsiderMinParallelism = ALL children need re-shuffle (none can avoid it)

candidateSpecs = specs where canCreatePartitioning == true
                 AND (if shouldConsiderMinParallelism: numPartitions >= conf.defaultShufflePartitions)

Prefer candidates without ShuffleExchangeLike (already-shuffled data)

bestSpec = candidate with highest numPartitions

For each child i in childrenIndexes:
  if bestSpec.isCompatibleWith(spec(i)) → child is already compatible, keep it
  else if bestSpec is Some(spec):
    newPartitioning = bestSpec.createPartitioning(clustering(i))
    child match:
      case ShuffleExchangeExec(_, c, so, ps) → replace partitioning: ShuffleExchangeExec(newPartitioning, c, so, ps)
      case _ → ShuffleExchangeExec(newPartitioning, child)
```

**Pass 3 — Ordering check:**
```
For each child i:
  if SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering(i)):
    → keep child as-is
  else:
    → SortExec(requiredOrdering(i), global = false, child = child)
```

### 5.3 Join Key Reordering

Before the distribution check, `EnsureRequirements` also reorders join keys to match the existing
partitioning of children. The intuition: if the left side is `HashPartitioning(b, a)` and the join
condition is `ON a = c AND b = d`, Spark can reorder to `ON b = d AND a = c` (matching the existing
partitioning) to avoid shuffling.

```scala
private def reorderJoinKeys(leftKeys, rightKeys, leftPartitioning, rightPartitioning) = {
  reorderJoinKeysRecursively(leftKeys, rightKeys, Some(leftPartitioning), Some(rightPartitioning))
    .getOrElse((leftKeys, rightKeys))
}
```

`reorderJoinKeysRecursively` tries to match `leftPartitioning` first (HashPartitioning, then
KeyedPartitioning, then PartitioningCollection), then falls back to `rightPartitioning`.

### 5.4 Special Case: `preferSinglePartition`

When all clustered-distribution children already have `SinglePartition` output AND the logical
stats say their size is ≤ `spark.sql.maxSinglePartitionBytes`, the co-partitioning optimization
is skipped and both sides are kept with `SinglePartition` (single-partition join).

---

## 6. ShuffleExchangeExec — Physical Execution In Detail

```
sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala
```

### 6.1 Constructor

```scala
case class ShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
    advisoryPartitionSize: Option[Long] = None)
  extends ShuffleExchangeLike
```

`advisoryPartitionSize` is set when the exchange comes from a `REBALANCE` hint, telling AQE the
target partition size when it rebalances. It is `None` for all other origins.

### 6.2 Key Lazy Fields

```
inputRDD         → child.execute()              — the upstream RDD of rows (not yet evaluated)
serializer       → UnsafeRowSerializer(n, ...)  — SQL's serializer (supports obj relocation)
shuffleDependency → prepareShuffleDependency(...)— the core RDD-level shuffle dependency
```

All three fields are `@transient lazy val` — they are computed on the executor that runs the
actual shuffle map task, not on the driver.

### 6.3 `prepareShuffleDependency` — The Central Algorithm

This is where the SQL `Partitioning` is translated into an RDD that produces `(partitionId, row)`
pairs. There are four logical steps:

#### Step 1: Choose a `Partitioner`

```scala
val part: Partitioner = newPartitioning match {
  case RoundRobinPartitioning(n) => new HashPartitioner(n)
  case HashPartitioning(_, n)    => new PartitionIdPassthrough(n)
  case ShufflePartitionIdPassThrough(_, n) => new PartitionIdPassthrough(n)
  case RangePartitioning(sortingExpressions, n) =>
    // Run a sampling job to compute range boundaries
    val rddForSampling = rdd.mapPartitionsInternal { iter =>
      val projection = UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
      val mutablePair = new MutablePair[InternalRow, Null]()
      iter.map(row => mutablePair.update(projection(row).copy(), null))
    }
    val orderingAttributes = sortingExpressions.zipWithIndex.map { case (ord, i) =>
      ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
    }
    implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
    new RangePartitioner(n, rddForSampling, ascending = true,
      samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
  case SinglePartition => new ConstantPartitioner
  case k: KeyedPartitioning => new KeyGroupedPartitioner(...)
}
```

For `RangePartitioning`, this triggers an **actual Spark job** to sample the data — only sort
key columns are collected (to minimize data movement during sampling). `RangePartitioner`
uses reservoir sampling internally, proportionally sampling ~20 points per partition.

#### Step 2: Build a `getPartitionKeyExtractor` function

This closure is called once per row to extract the partition key:

```scala
def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
  case RoundRobinPartitioning(numPartitions) =>
    // XORShiftRandom seeded by task's partitionId, incremented each row
    val partitionId = TaskContext.get().partitionId()
    var position = new XORShiftRandom(partitionId).nextInt(numPartitions)
    (row: InternalRow) => { position += 1; position }

  case h: HashPartitioning =>
    // Evaluate partitionIdExpression = Pmod(Murmur3Hash(exprs), numPartitions)
    val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
    row => projection(row).getInt(0)

  case RangePartitioning(sortingExpressions, _) =>
    // Extract sort key columns only
    val projection = UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
    row => projection(row)

  case SinglePartition => identity
}
```

For `RoundRobinPartitioning`, each task starts at a random offset (seeded by its own `partitionId`)
to avoid all tasks starting at partition 0 and creating load imbalance.

#### Step 3: Handle `sortBeforeRepartition` for RoundRobin

Round-robin repartitioning is **order-sensitive**: a retry task might produce rows in a different
order, which would corrupt deduplication semantics. To make it deterministic:

```scala
val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
  rdd.mapPartitionsInternal { iter =>
    // Sort rows by their binary hash before assigning partition IDs
    val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
      schema, RecordBinaryComparator, PrefixComparators.LONG,
      prefixComputer = row => result.value = row.hashCode(), ...)
    sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
  }
} else rdd
```

The sort is by the row's binary `hashCode()` as a sort key prefix, using
`RecordBinaryComparator` for byte-level comparison. This ensures identical rows always sort
together regardless of which task runs them.

#### Step 4: Decide whether to copy rows

`needToCopyObjectsBeforeShuffle` decides whether rows need defensive copying before shuffle:

```scala
private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
  val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
  val bypassMergeThreshold = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
  if (sortBasedShuffleOn) {
    if (numParts <= bypassMergeThreshold) false       // bypass path: no buffering
    else if (numParts <= MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) false
    else true   // BaseShuffleHandle: ExternalSorter buffers objects, must copy
  } else true
}
```

SQL operators return the **same mutable `InternalRow` object** for each call to `next()`.
If the shuffle write path buffers rows in memory (as `ExternalSorter` does in deserialized mode),
later reads of the buffered row would see stale data. When a copy is needed, the pipeline is:
```
rdd.mapPartitionsWithIndexInternal { (_, iter) =>
  iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
}
```
When no copy is needed, a reusable `MutablePair` avoids object allocation:
```
iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
```

#### Step 5: Create `ShuffleDependency`

```scala
val dependency = new ShuffleDependency[Int, InternalRow, InternalRow](
  rddWithPartitionIds,
  new PartitionIdPassthrough(part.numPartitions),
  serializer,                                // UnsafeRowSerializer
  shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
  rowBasedChecksums = UnsafeRowChecksum.createUnsafeRowChecksums(checksumSize),
  ...)
```

The final `ShuffleDependency` always uses `PartitionIdPassthrough` because the partition IDs
were already pre-computed in `rddWithPartitionIds`. The inner `part` Partitioner was only
needed to compute those IDs.

### 6.4 Execution Flow

```
doExecute()
  → new ShuffledRowRDD(shuffleDependency, readMetrics)
      — uses default CoalescedPartitionSpec(i, i+1) for each partition

With AQE:
  submitShuffleJob()
    → executeQuery(null)  [waits for subqueries]
    → sparkContext.submitMapStage(shuffleDependency)  → Future[MapOutputStatistics]

  AQE re-optimizes using MapOutputStatistics.bytesByPartitionId

  getShuffleRDD(partitionSpecs)
    → new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
      — partitionSpecs may coalesce or split partitions
```

### 6.5 Metrics

`ShuffleExchangeExec` tracks the following SQL metrics:

| Metric | Meaning |
|---|---|
| `dataSize` | Total bytes written across all map tasks (reported by UnsafeRowSerializer) |
| `numPartitions` | Shuffle output partition count |
| `shuffleRecordsWritten` | Rows written by map tasks |
| `shuffleWriteTime` | Wall-clock time for writing |
| `fetchWaitTime` | Time reduce tasks spent waiting for remote blocks |
| `remoteBlocksFetched` | Count of blocks fetched over the network |
| `localBlocksFetched` | Count of blocks read from local disk |

---

## 7. BroadcastExchangeExec — Broadcast Joins

```
sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala
```

Broadcast exchanges avoid shuffle entirely for small tables. The entire relation is collected to
the driver and broadcast to all executors as an in-memory structure.

```scala
case class BroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
  extends BroadcastExchangeLike
```

### 7.1 Execution Model — Fully Asynchronous

`BroadcastExchangeExec` uses a two-future model to decouple preparation from materialization:

```
doPrepare()
  → materializes relationFuture (starts background thread)

relationFuture (lazy val, runs in BroadcastExchangeExec.executionContext):
  1. sparkContext.addJobTag(jobTag)  — for cancellation
  2. child.executeCollectIterator() — fetches all rows to driver
     If numRows >= maxBroadcastRows → throw exception
  3. mode.transform(input, Some(numRows)) → builds broadcast relation in memory
     Measures dataSize:
       HashedRelation → map.estimatedSize
       Array[InternalRow] → sum of UnsafeRow.getSizeInBytes
     If dataSize >= maxBroadcastTableSizeInBytes → throw exception
  4. sparkContext.broadcastInternal(relation, serializedOnly = true)
     — serializes and distributes to all executors (no local cache)
  5. promise.trySuccess(broadcasted)

doExecuteBroadcast():
  → relationFuture.get(timeout, TimeUnit.SECONDS)  — blocks up to broadcastTimeout
```

### 7.2 Row and Byte Limits

| Limit | Config | Default |
|---|---|---|
| Max rows | N/A (hardcoded) | `BytesToBytesMap.MAX_CAPACITY / 1.5 ≈ 341M` for multi-key; `512M` for single Long key |
| Max bytes | `spark.sql.autoBroadcastJoinThreshold` | 10 MB (also controls whether broadcast is chosen) |
| Broadcast timeout | `spark.sql.broadcastTimeout` | 300 seconds |

### 7.3 `BroadcastMode`

Controls how the collected rows are transformed into the broadcast structure:

```
BroadcastMode
├── HashedRelationBroadcastMode(key, isNullAware)
│   → builds a HashedRelation (used for hash joins)
│   → key = join key expressions
│   → isNullAware = true for null-aware anti-joins
│
└── IdentityBroadcastMode
    → passes rows as Array[InternalRow]
    → used for nested-loop joins (no key)
```

`HashedRelation` has two implementations chosen at build time:
- `LongHashedRelation` — single join key of `LongType`; uses a dense array indexed by long value
- `UnsafeHashedRelation` — general case; backed by `BytesToBytesMap` (off-heap hash map)

### 7.4 Cancellation

The broadcast job is tagged with `jobTag = "broadcast exchange (runId ${runId})"`. When the
query fails or is cancelled, `cancelBroadcastJob` calls
`sparkContext.cancelJobsWithTag(jobTag)` to kill the running collection job.

---

## 8. ReusedExchangeExec — Exchange Reuse

```
sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/Exchange.scala
```

When two parts of a query plan produce logically identical shuffle output (same partitioning and
same child subplan), Spark reuses the exchange rather than computing it twice.

```scala
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode
```

`ReusedExchangeExec` is a `LeafExecNode` — it has no children in the plan tree — but
delegates all execution to its `child` Exchange. It carries a **different set of output
attribute IDs** from its child, because the two references to the exchange were derived through
different plan paths and thus have different attribute IDs.

### 8.1 Attribute ID Remapping Algorithm

```scala
private[sql] lazy val updateAttr: Expression => Expression = {
  val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
  e => e.transform {
    case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
  }
}

override def outputPartitioning: Partitioning = child.outputPartitioning match {
  case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
  case other => other
}

override def outputOrdering: Seq[SortOrder] =
  child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
```

The remapping is needed because `HashPartitioning` is an `Expression` containing `AttributeReference`
nodes with specific `ExprId`s. If downstream operators reference the reused exchange's output
attributes (which have different `ExprId`s), the partitioning expression must be rewritten to
use those new IDs.

### 8.2 Where Reuse Is Applied

`ReuseExchangeAndSubquery` is a physical plan rule that:
1. Traverses the plan and collects all `Exchange` nodes.
2. Groups them by `canonicalized` plan equality (`sameResult`).
3. For duplicates, replaces the second and subsequent occurrences with
   `ReusedExchangeExec(mappedOutput, firstExchange)`.

The equality check uses `canonicalized` (which normalizes attribute IDs and expression orderings)
so that logically equivalent exchanges built from different plan branches are recognized as equal.

---

## 9. Core Shuffle: ShuffleDependency

`ShuffleDependency` is the RDD-level object that represents the dependency between a mapper stage
and a reducer stage. It lives in `core/` and knows nothing about SQL.

```scala
// core/src/main/scala/org/apache/spark/Dependency.scala
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
  extends Dependency[C]
```

For Spark SQL, the types are always `ShuffleDependency[Int, InternalRow, InternalRow]`:
- `K = Int` — the partition ID (pre-computed in `rddWithPartitionIds`)
- `V = InternalRow` — the actual row data
- `C = InternalRow` — same type; SQL never uses map-side combine

Key fields SQL sets:
- `partitioner` = always `PartitionIdPassthrough(n)` (IDs already computed)
- `serializer` = `UnsafeRowSerializer` (supports object relocation → enables Tungsten sort path)
- `keyOrdering` = `None` (SQL pre-sorts via `SortExec` if needed)
- `aggregator` = `None`
- `mapSideCombine` = `false`

### 9.1 Registration and `shuffleId`

When the `ShuffleDependency` is created on the driver, it calls
`SparkContext.newShuffleId()` to acquire a monotonically increasing `shuffleId`. This ID is used by
`MapOutputTracker` to track which mappers have completed and where their output blocks are.

When a stage is submitted, `SparkContext` calls
`ShuffleManager.registerShuffle(shuffleId, dep)`, which returns a `ShuffleHandle`. The handle
type determines the write path (see Section 10).

---

## 10. SortShuffleManager and the Three Write Paths

`SortShuffleManager` is Spark's default (and only built-in) shuffle manager. The choice of write
path is made once at `registerShuffle` time and encoded in the `ShuffleHandle` type.

```
core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala
```

### 10.1 Write Path Selection

```
registerShuffle(shuffleId, dependency):
  if SortShuffleWriter.shouldBypassMergeSort(conf, dep):
    → BypassMergeSortShuffleHandle  (bypass path)
  else if SortShuffleManager.canUseSerializedShuffle(dep):
    → SerializedShuffleHandle       (Tungsten / serialized sort path)
  else:
    → BaseShuffleHandle             (deserialized sort path)

getWriter(handle, mapId, ...):
  match handle:
    SerializedShuffleHandle   → UnsafeShuffleWriter
    BypassMergeSortShuffleHandle → BypassMergeSortShuffleWriter
    BaseShuffleHandle         → SortShuffleWriter
```

### 10.2 Path 1: Bypass Merge-Sort (`BypassMergeSortShuffleWriter`)

**Condition:** `numPartitions <= spark.shuffle.sort.bypassMergeThreshold` (default 200)
**AND** `!dep.mapSideCombine` (no map-side aggregation).

SQL always satisfies the `mapSideCombine = false` condition, so this path is used for any SQL
shuffle with ≤ 200 output partitions.

**Algorithm:**
1. Open **one `DiskBlockObjectWriter` per output partition** — so `numPartitions` file handles
   are open simultaneously.
2. For each incoming `(partitionId, row)` pair, write the row directly to the writer for
   `partitionId` using the serializer.
3. After processing all rows: flush all writers and get a `FileSegment` (file + offset + length)
   per partition.
4. Call `shuffleBlockResolver.writeIndexFileAndCommit(...)` to:
   - Concatenate all partition files into a single `.data` file.
   - Write the `.index` file with `numPartitions + 1` offsets.
   - Atomically move the files into place.

**Pros:** No sorting; no serialization/deserialization cycle for merging; fast for small partition counts.  
**Cons:** Holding `numPartitions` file handles simultaneously limits scalability.

### 10.3 Path 2: Serialized Sort / Tungsten (`UnsafeShuffleWriter`)

**Condition:** `dep.serializer.supportsRelocationOfSerializedObjects` (true for `UnsafeRowSerializer`)
**AND** `!dep.mapSideCombine`
**AND** `numPartitions <= MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE` (16,777,216).

**Algorithm:**
1. Use `ShuffleExternalSorter` to sort records in a **serialized binary form**:
   - Records are serialized immediately upon arrival using the shuffler's `SerializationStream`.
   - A `LongArray` is maintained where each 8-byte entry packs `(partitionId << 24) | recordPointer`.
     This fits more entries in CPU cache than a Java object array.
   - When memory is exhausted, sort the `LongArray` and spill to a `SpillFile` on disk without
     deserializing rows.
2. Once all records are processed, do a final sort of in-memory records.
3. Merge all spill files plus the in-memory buffer:
   - If the codec supports **concatenation** (e.g., LZ4, Snappy), compressed spill segments
     for the same partition are directly concatenated using `NIO transferTo` — no decompression.
   - Otherwise, decompress and re-compress.
4. Write the final `.data` and `.index` files.

**Key advantage:** The sort operates on 8-byte packed pointers, not Java objects → fewer GC pauses
and better cache utilization. The serialized merge avoids a full deserialization round-trip.

### 10.4 Path 3: Deserialized Sort (`SortShuffleWriter` + `ExternalSorter`)

**Condition:** All other cases — e.g., `mapSideCombine = true` (RDD-level `combineByKey`), or
the serializer does not support object relocation.

Spark SQL always lands on Path 1 or Path 2 because `UnsafeRowSerializer` supports relocation and
SQL never uses map-side combine. Path 3 is primarily used by Spark Core's RDD API.

**Algorithm (`ExternalSorter`):**
1. Insert records into an in-memory buffer (`PartitionedAppendOnlyMap` if aggregating,
   `PartitionedPairBuffer` otherwise).
2. If memory is exhausted, sort the buffer by `(partitionId, key)` and spill to disk as a
   sorted file.
3. At the end, do a merge-sort of all spill files plus the in-memory buffer.
4. Write the sorted output as a single `.data` file + `.index` file.

**The merging cost is higher** because deserialized records must be re-serialized during spill
and deserialized again during merge.

### 10.5 Shuffle Reader (shared by all three write paths)

All three write paths produce the same file format (`.data` + `.index`), so they share the same
reader: `BlockStoreShuffleReader`.

```
BlockStoreShuffleReader.read():
  1. Query MapOutputTracker:
     if shuffle is push-merged: getPushBasedShuffleMapSizesByExecutorId(shuffleId, ...)
     else:                       getMapSizesByExecutorId(shuffleId, startPart, endPart, ...)
     → returns: Seq[(BlockManagerId, Seq[(BlockId, size, mapIndex)])]

  2. ShuffleBlockFetcherIterator:
     - Local blocks: read directly from local BlockManager (disk)
     - Remote blocks: fetch over network using TransportClient
     - Manages concurrent fetch requests, respects maxBytesInFlight limit

  3. Deserialize each block using dep.serializer
  4. If dep.aggregator defined: apply reduce-side combine (not used by SQL)
  5. If dep.keyOrdering defined: apply final merge sort (not used by SQL)
  6. Return iterator of (K, C) pairs = (Int, InternalRow) for SQL
```

---

## 11. IndexShuffleBlockResolver — Map Output Storage

`IndexShuffleBlockResolver` manages the actual files written by map tasks.

```
core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala
```

### 11.1 File Layout

For each map task with `shuffleId` and `mapId`:

- **Data file**: `${blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, 0))}`  
  Naming: `shuffle_{shuffleId}_{mapId}_0.data`  
  Content: All partition data concatenated back-to-back (no separators).

- **Index file**: `shuffle_{shuffleId}_{mapId}_0.index`  
  Content: `numPartitions + 1` `Long` values (8 bytes each).  
  - `index[0]` = 0 (start of first partition)
  - `index[i]` = byte offset where partition `i` starts
  - `index[numPartitions]` = total file length
  - Length of partition `i` = `index[i+1] - index[i]`

So a reducer reading partition `r` does:
```
seek to index[r] in .data file, read (index[r+1] - index[r]) bytes
```

This is why `SortShuffleManager` only needs **2 files** per map task regardless of partition count,
and reducers can do a single sequential read per mapper.

### 11.2 Checksum Files (optional)

When `spark.shuffle.checksum.enabled = true`:
- `shuffle_{shuffleId}_{mapId}_0.checksums`  
  Contains one `Long` checksum per partition.

On the read side, if a checksum mismatch is detected, Spark can:
- With `spark.shuffle.checksum.mismatchFullRetryEnabled = true`: re-fetch and verify the full partition
- With `spark.shuffle.checksum.mismatchQueryLevelRollbackEnabled = true`: roll back the entire query

### 11.3 Atomic Commit Protocol

`writeIndexFileAndCommit` uses an atomic rename pattern to avoid partial writes:
1. Write the `.data` file to a **temporary path** first.
2. Write the `.index` file to a **temporary path**.
3. Check if a committed index file already exists (task retry case):
   - If the existing index has the same content → use the existing files, delete temporaries.
   - Otherwise → rename temporaries to the final paths atomically.

This ensures that a reducer that reads the index file always sees a complete, consistent write.

---

## 12. ShuffledRowRDD and ShufflePartitionSpec

`ShuffledRowRDD` is the RDD that reads shuffle output on the reduce side. It is a SQL-specific
replacement for `ShuffledRDD` that works directly on `InternalRow` rather than `(K, V)` pairs.

```
sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala
```

### 12.1 Constructor

```scala
class ShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])
  extends RDD[InternalRow]
```

The default constructor (no `partitionSpecs`) creates one `CoalescedPartitionSpec(i, i+1)` per
shuffle partition — a standard 1:1 mapping between reduce tasks and shuffle partitions.

### 12.2 `ShufflePartitionSpec` — The AQE Extension Point

```scala
sealed trait ShufflePartitionSpec

case class CoalescedPartitionSpec(
    startReducerIndex: Int, endReducerIndex: Int, dataSize: Option[Long])
  extends ShufflePartitionSpec

case class PartialReducerPartitionSpec(
    reducerIndex: Int, startMapIndex: Int, endMapIndex: Int, dataSize: Long)
  extends ShufflePartitionSpec

case class PartialMapperPartitionSpec(
    mapIndex: Int, startReducerIndex: Int, endReducerIndex: Int)
  extends ShufflePartitionSpec

case class CoalescedMapperPartitionSpec(
    startMapIndex: Int, endMapIndex: Int, numReducers: Int)
  extends ShufflePartitionSpec
```

#### How `ShuffledRowRDD.compute()` handles each spec

```scala
override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
  val reader = split.asInstanceOf[ShuffledRowRDDPartition].spec match {
    case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
      // Read partitions [startReducerIndex, endReducerIndex) from ALL mappers
      shuffleManager.getReader(dep.shuffleHandle, startReducerIndex, endReducerIndex, ...)

    case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
      // Read partition reducerIndex from only mappers [startMapIndex, endMapIndex)
      shuffleManager.getReader(dep.shuffleHandle, startMapIndex, endMapIndex,
                               reducerIndex, reducerIndex + 1, ...)

    case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
      // Read partitions [startReducerIndex, endReducerIndex) from mapper mapIndex only
      shuffleManager.getReader(dep.shuffleHandle, mapIndex, mapIndex + 1,
                               startReducerIndex, endReducerIndex, ...)

    case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
      shuffleManager.getReader(dep.shuffleHandle, startMapIndex, endMapIndex,
                               0, numReducers, ...)
  }
  reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
}
```

Each spec maps to a different rectangle of the `(mappers × reducers)` output matrix:
- `CoalescedPartitionSpec`: **full column range** across all mappers for a reducer range
- `PartialReducerPartitionSpec`: **partial column** — only some mappers for one reducer (skew split)
- `PartialMapperPartitionSpec`: **partial row** — one mapper for a reducer range
- `CoalescedMapperPartitionSpec`: **partial row range** — several mappers for all reducers

### 12.3 Partition Locality

`getPreferredLocations` queries `MapOutputTrackerMaster` to find where the relevant map outputs
are stored, enabling HDFS-style data locality for local reads:

```scala
case CoalescedPartitionSpec(start, end, _) =>
  start.until(end).flatMap { r => tracker.getPreferredLocationsForShuffle(dep, r) }
case PartialReducerPartitionSpec(_, startMap, endMap, _) =>
  tracker.getMapLocation(dep, startMap, endMap)
```

### 12.4 AQE Usage of Each Spec Type

| Spec | AQE rule that creates it | Purpose |
|---|---|---|
| `CoalescedPartitionSpec(0, n)` | Default (no AQE) | One task per original partition |
| `CoalescedPartitionSpec(i, j)` | `CoalesceShufflePartitions` | Merge many small partitions into one task |
| `PartialReducerPartitionSpec` | `OptimizeSkewedJoin` | Split one large partition across multiple tasks |
| `PartialMapperPartitionSpec` | Streaming / local sort merge | Read one mapper's contribution to a key range |
| `CoalescedMapperPartitionSpec` | Streaming stateful ops | Merge mapper outputs for a reducer range |

---

## 13. MapOutputStatistics and AQE Integration

`MapOutputStatistics` carries per-partition byte counts from a completed shuffle stage:

```scala
class MapOutputStatistics(val shuffleId: Int, val bytesByPartitionId: Array[Long])
```

### 13.1 Collection

After all map tasks finish, the driver's `MapOutputTracker` aggregates output sizes.
`sparkContext.submitMapStage(shuffleDependency)` returns a `FutureAction[MapOutputStatistics]`.
When the action completes, `bytesByPartitionId(i)` contains the total bytes all mappers wrote
for reduce partition `i`.

### 13.2 AQE Decision Flow

```
1. ShuffleExchangeExec.submitShuffleJob()
     → sparkContext.submitMapStage(dep) → Future[MapOutputStatistics]

2. AdaptiveSparkPlanExec awaits all MapOutputStatistics for the current stage

3. AQEOptimizer.reOptimize():
   - CoalesceShufflePartitions rule:
       Reads bytesByPartitionId, greedily merges consecutive small partitions until
       combined size ≥ advisoryPartitionSizeInBytes.
       Produces CoalescedPartitionSpec(start, end, dataSize) per merged group.

   - OptimizeSkewedJoin rule:
       Finds partitions where bytesByPartitionId[i] > median × skewFactor
                               AND bytesByPartitionId[i] > skewedPartitionThreshold
       Splits such partitions into PartialReducerPartitionSpec sub-tasks,
       each reading a subset of mappers.

4. AQEShuffleReadExec is inserted on top of the exchange with the new specs.
   Its getShuffleRDD(specs) calls ShuffleExchangeExec.getShuffleRDD(specs)
     → new ShuffledRowRDD(dep, metrics, specs)
```

### 13.3 The `submitShuffleJob` Async Model

```
submitShuffleJob():
  triggerFuture (java.util.concurrent.Future):
    Runs on ShuffleExchangeExec.executionContext thread:
      executeQuery(null)          // wait for subqueries
      synchronized:
        if isCancelled → promise.tryFailure(...)
        else:
          action = sparkContext.submitMapStage(dep)   // returns FutureAction
          futureAction.set(Some(action))
          promise.completeWith(action)

  return completionFuture  // Scala Future backed by promise
```

The `synchronized` block ensures that if `cancelShuffleJob()` is called between the trigger
and the submission, the cancel is not lost.

---

## 14. Key Configuration Properties

| Property | Default | Effect |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Number of reduce partitions for SQL shuffles |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Max table size for automatic broadcast join |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | Max partitions to use bypass-merge write path |
| `spark.sql.sortBeforeRepartition` | true | Sort before round-robin repartition for determinism |
| `spark.sql.adaptive.enabled` | true | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | AQE: coalesce small shuffle partitions |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1MB | AQE: minimum coalesced partition size |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | AQE: target partition size for coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | true | AQE: split skewed partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5.0 | AQE: factor over median to be considered skewed |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | AQE: minimum bytes to be considered skewed |
| `spark.shuffle.checksum.enabled` | true | Write per-partition checksums |
| `spark.shuffle.checksum.mismatchFullRetryEnabled` | false | Re-fetch partition on checksum mismatch |
| `spark.sql.exchange.reuse` | true | Enable exchange reuse (ReusedExchangeExec) |
| `spark.sql.broadcastTimeout` | 300 | Seconds to wait for a broadcast to complete |
| `spark.sql.maxSinglePartitionBytes` | 20MB | Max size for single-partition join optimization |
| `spark.sql.requireAllClusterKeysForDistribution` | false | Require exact key match for ClusteredDistribution |
| `spark.sql.rangeExchange.sampleSizePerPartition` | 100 | Sampling points per partition for RangePartitioner |

---

## 15. Key Source Files

| File | Purpose |
|---|---|
| `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/Exchange.scala` | Base `Exchange` class, `ReusedExchangeExec` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala` | `ShuffleExchangeLike`, `ShuffleOrigin`, `ShuffleExchangeExec`, `prepareShuffleDependency` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala` | `BroadcastExchangeLike`, `BroadcastExchangeExec` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala` | Rule inserting exchanges and sorts; co-partitioning optimization; join key reorder |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala` | `Distribution`, `Partitioning`, `ShuffleSpec` hierarchies; `satisfies()` logic |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala` | `ShuffledRowRDD`, `ShufflePartitionSpec` types, `compute()` per spec |
| `core/src/main/scala/org/apache/spark/Dependency.scala` | `ShuffleDependency` |
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | `ShuffleManager` pluggable interface |
| `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` | `SortShuffleManager`, write path handle selection |
| `core/src/main/scala/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java` | Bypass-merge write path implementation |
| `core/src/main/scala/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java` | Serialized/Tungsten sort write path |
| `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala` | Deserialized sort write path |
| `core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala` | Map output file storage (data + index files), atomic commit |
| `core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala` | Shuffle read side, block fetching |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala` | AQE execution loop, uses `mapOutputStatisticsFuture` |
