# From SQL Query to RDD: Spark SQL Execution and Codegen

This guide traces the complete path from a SQL string (or DataFrame API call) to distributed
execution on executors — covering the planning pipeline, how `SparkPlan` nodes become RDDs,
which RDD classes are involved, and how Whole-Stage Code Generation (WSCG) fuses operator pipelines
into compiled Java code.

---

## Table of Contents

1. [The Execution Pipeline Overview](#1-the-execution-pipeline-overview)
2. [QueryExecution — The Orchestrator](#2-queryexecution--the-orchestrator)
3. [SparkPlan — The Physical Operator Abstraction](#3-sparkplan--the-physical-operator-abstraction)
4. [The Preparations Phase — Rules Applied Before Execution](#4-the-preparations-phase--rules-applied-before-execution)
5. [SparkPlan to RDD: `doExecute()` Implementations](#5-sparkplan-to-rdd-doexecute-implementations)
6. [Key RDD Classes in Spark SQL](#6-key-rdd-classes-in-spark-sql)
7. [How a Row Is Represented: InternalRow and UnsafeRow](#7-how-a-row-is-represented-internalrow-and-unsaferow)
8. [Whole-Stage Code Generation (WSCG)](#8-whole-stage-code-generation-wscg)
9. [CodegenContext — The Code Generation State Machine](#9-codegencontext--the-code-generation-state-machine)
10. [ExprCode — Code for One Expression Evaluation](#10-exprcode--code-for-one-expression-evaluation)
11. [Operator-Level Codegen: Produce/Consume Protocol](#11-operator-level-codegen-produceconsume-protocol)
12. [Concrete Codegen Examples](#12-concrete-codegen-examples)
13. [Janino Compilation and Code Cache](#13-janino-compilation-and-code-cache)
14. [Codegen Fallbacks and Safety Valves](#14-codegen-fallbacks-and-safety-valves)
15. [End-to-End Example: SELECT with Filter and Project](#15-end-to-end-example-select-with-filter-and-project)
16. [Key Configuration Properties](#16-key-configuration-properties)
17. [Key Source Files](#17-key-source-files)

---

## 1. The Execution Pipeline Overview

A SQL query passes through the following phases, all lazy until results are actually consumed:

```
SQL text / DataFrame API
        │
        ▼
  Unresolved LogicalPlan  ──  Parser (ANTLR grammar in sql/catalyst/src/main/antlr4/)
        │
        ▼  Analyzer (sql/catalyst/.../analysis/Analyzer.scala)
           Resolves attribute references, functions, types using Catalog.
           Output: Analyzed LogicalPlan
        │
        ▼  Optimizer (sql/catalyst/.../optimizer/Optimizer.scala)
           Rule-based rewrites: constant folding, predicate pushdown, join reordering, etc.
           Output: Optimized LogicalPlan
        │
        ▼  SparkPlanner  (sql/core/.../execution/SparkStrategies.scala)
           Pattern-matches LogicalPlan nodes into SparkPlan (physical operators).
           Output: SparkPlan (unexecuted)
        │
        ▼  preparations: Seq[Rule[SparkPlan]]
           EnsureRequirements, CollapseCodegenStages, ReuseExchangeAndSubquery, etc.
           Output: executedPlan (SparkPlan ready to execute)
        │
        ▼  executedPlan.execute()
           Recursively calls doExecute() → builds RDD DAG
        │
        ▼  RDD[InternalRow] → Spark Action → distributed execution on executors
```

The entire pipeline is implemented in `QueryExecution`. The key insight is that
**building the RDD DAG is a driver-side operation** — it constructs the computation graph but
does not move any data. Data movement happens only when a Spark action (`.collect()`, `.count()`,
`.save()`, etc.) triggers job submission.

---

## 2. QueryExecution — The Orchestrator

`QueryExecution` is the internal class that holds all the lazy vals corresponding to each phase:

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker,
    val mode: CommandExecutionMode.Value = CommandExecutionMode.ALL)
```

### Lazy Evaluation Chain

Each phase is a `LazyTry` — computed once on first access, result cached, error re-thrown on
subsequent accesses. Accessing any phase forces all preceding phases:

```
logical
  └─ analyzed         (Analyzer.execute(logical))
      └─ optimizedPlan (Optimizer.execute(analyzed))
          └─ sparkPlan  (SparkPlanner.plan(optimizedPlan).next())
              └─ executedPlan (prepareForExecution(preparations, sparkPlan.clone()))
                  └─ toRdd     (SQLExecutionRDD(executedPlan.execute(), conf))
```

### `prepareForExecution`

```scala
private[execution] def prepareForExecution(
    preparations: Seq[Rule[SparkPlan]],
    plan: SparkPlan): SparkPlan = {
  val planChangeLogger = new PlanChangeLogger[SparkPlan]()
  val preparedPlan = preparations.foldLeft(plan) { case (sp, rule) =>
    val result = rule.apply(sp)
    planChangeLogger.logRule(rule.ruleName, sp, result)
    result
  }
  planChangeLogger.logBatch("Preparations", plan, preparedPlan)
  preparedPlan
}
```

The preparations rules are applied left-to-right, each rule receiving the output of the previous
one. The `planChangeLogger` records before/after for each rule, visible via
`spark.sql.planChangeLog.level = WARN`.

### `toRdd`

```scala
val lazyToRdd = LazyTry {
  new SQLExecutionRDD(executedPlan.execute(), sparkSession.sessionState.conf)
}
def toRdd: RDD[InternalRow] = lazyToRdd.get
```

`toRdd` wraps the result in `SQLExecutionRDD`, which propagates `SQLConf` thread-local state
to executor threads. This is the entry point for all downstream DataFrame/Dataset operations.

---

## 3. SparkPlan — The Physical Operator Abstraction

```scala
// sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable
```

`SparkPlan` is a `TreeNode[SparkPlan]` extended with execution semantics. Every physical operator
is a `SparkPlan` subclass.

### Key Methods

| Method | Description |
|---|---|
| `execute(): RDD[InternalRow]` | Public entry point; calls `doExecute()` once and caches result |
| `doExecute(): RDD[InternalRow]` | Abstract; each operator implements how it produces its RDD |
| `executeQuery[T](query: => T): T` | Template method: calls `prepare()`, waits for subqueries, then runs `query` |
| `prepare()` | Recursive pre-execution setup: prepares subqueries, calls `doPrepare()` on each node |
| `executeBroadcast[T]()` | For broadcast joins; calls `doExecuteBroadcast()` |
| `executeColumnar()` | For vectorized columnar execution; calls `doExecuteColumnar()` |

### `execute()` in detail

```scala
@transient
private val executeRDD = LazyTry { doExecute() }

final def execute(): RDD[InternalRow] = executeQuery {
  if (isCanonicalizedPlan) throw SparkException.internalError(...)
  executeRDD.get
}
```

The `LazyTry` wrapper ensures `doExecute()` is called at most once. The `executeQuery` template:

```scala
protected final def executeQuery[T](query: => T): T = {
  RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
    prepare()         // prepare subqueries, call doPrepare() recursively
    waitForSubqueries() // block until scalar subqueries complete
    query             // call doExecute() (via executeRDD.get)
  }
}
```

`RDDOperationScope.withScope` tags all RDDs created inside with the operator name, which
appears in the Spark UI's DAG visualization.

### Node Structural Traits

Physical operators follow the same structural hierarchy as logical plans:

| Trait | Children | Examples |
|---|---|---|
| `LeafExecNode` | 0 | `FileSourceScanExec`, `RDDScanExec`, `LocalTableScanExec`, `RangeExec` |
| `UnaryExecNode` | 1 | `ProjectExec`, `FilterExec`, `SortExec`, `HashAggregateExec` |
| `BinaryExecNode` | 2 | `SortMergeJoinExec`, `BroadcastHashJoinExec`, `ShuffledHashJoinExec` |

---

## 4. The Preparations Phase — Rules Applied Before Execution

`QueryExecution.preparations` defines the ordered sequence of `Rule[SparkPlan]` that transform
the raw `sparkPlan` into the `executedPlan`:

```scala
private[execution] def preparations(...): Seq[Rule[SparkPlan]] =
  adaptiveExecutionRule.toSeq ++
  Seq(
    CoalesceBucketsInJoin,
    PlanDynamicPruningFilters(sparkSession),   // runtime dynamic partition pruning
    PlanSubqueries(sparkSession),              // convert scalar subqueries to ExecSubqueryExpression
    RemoveRedundantProjects,
    EnsureRequirements(),                      // insert ShuffleExchangeExec and SortExec
    InsertSortForLimitAndOffset,
    ReplaceHashWithSortAgg,
    RemoveRedundantSorts,
    RemoveRedundantWindowGroupLimits,
    DisableUnnecessaryBucketedScan,
    ApplyColumnarRulesAndInsertTransitions(...),
    CollapseCodegenStages()                    // wrap codegen-eligible subtrees in WholeStageCodegenExec
  ) ++
  Seq(ReuseExchangeAndSubquery)               // share identical Exchange nodes
```

The two most important rules for understanding execution are:

### `EnsureRequirements`
Inserts `ShuffleExchangeExec` and `SortExec` nodes wherever operator distribution/ordering
requirements are not met. See `docs/shuffle-and-exchanges-guide.md` for details.

### `CollapseCodegenStages`
Finds chains of `CodegenSupport` operators and wraps each chain in a `WholeStageCodegenExec`.
Exchange nodes act as natural boundaries between codegen stages. See Section 8.

---

## 5. SparkPlan to RDD: `doExecute()` Implementations

Each physical operator implements `doExecute()` to return an `RDD[InternalRow]`. The pattern
is always: apply a transformation to the child's `RDD` using `mapPartitions*`.

### Leaf operators — produce RDDs from external data

```scala
// FileSourceScanExec: reads from HDFS/S3/local via FileScanRDD
override def doExecute(): RDD[InternalRow] = {
  ...
  new FileScanRDD(sparkSession, readFunction, filePartitions, ...) // see Section 6
}

// RangeExec: generates integer rows
override def doExecute(): RDD[InternalRow] = {
  sqlContext.sparkContext.range(start, end, step, numSlices)
    .mapPartitionsWithIndex { ... }
}

// LocalTableScanExec: returns in-memory rows (driver-local)
override def doExecute(): RDD[InternalRow] = {
  val numPartitions = ...
  sqlContext.sparkContext.parallelize(unsafeRows.toSeq, numPartitions)
}
```

### Unary operators — transform child's RDD

```scala
// ProjectExec: evaluated lazily; with codegen this path is rarely taken
override protected def doExecute(): RDD[InternalRow] = {
  val evaluatorFactory = new ProjectEvaluatorFactory(projectList, child.output)
  child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
    val evaluator = evaluatorFactory.createEvaluator()
    evaluator.eval(index, iter)
  }
}

// FilterExec: wraps child RDD with a predicate filter
override protected def doExecute(): RDD[InternalRow] = {
  val evaluatorFactory = new FilterEvaluatorFactory(condition, child.output, numOutputRows)
  child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
    val evaluator = evaluatorFactory.createEvaluator()
    evaluator.eval(index, iter)
  }
}

// SortExec: wraps child RDD with a per-partition sort
override def doExecute(): RDD[InternalRow] = {
  child.execute().mapPartitionsWithIndexInternal { (i, iter) =>
    val sorter = createSorter()
    val array = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
    array.toScala
  }
}
```

### Binary operators (joins, unions)

```scala
// ShuffleExchangeExec: the shuffle boundary — produces a ShuffledRowRDD
override protected def doExecute(): RDD[InternalRow] =
  new ShuffledRowRDD(shuffleDependency, readMetrics)

// SortMergeJoinExec: merges two sorted RDDs
override def doExecute(): RDD[InternalRow] = {
  val leftRDD = left.execute()
  val rightRDD = right.execute()
  leftRDD.zipPartitions(rightRDD) { (leftIter, rightIter) =>
    new RowIterator { ... } // merge-join logic
  }
}
```

### `WholeStageCodegenExec` — the special case

When codegen is active, most of the above `doExecute()` implementations are never called.
Instead, `WholeStageCodegenExec.doExecute()` generates a single fused Java class that
replaces the whole subtree (see Section 8).

---

## 6. Key RDD Classes in Spark SQL

### `SQLExecutionRDD`

```
sql/core/src/main/scala/org/apache/spark/sql/execution/SQLExecutionRDD.scala
```

Thin wrapper around `RDD[InternalRow]` that propagates `SQLConf` thread-local state from
the driver to executor threads. Every `QueryExecution.toRdd` is wrapped in this.

### `FileScanRDD`

```
sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala
```

The RDD for file-based data sources (Parquet, ORC, CSV, JSON, etc.). Each partition is a
`FilePartition` — a list of `PartitionedFile` objects (a file path + byte range).

```scala
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: PartitionedFile => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    override val readSchema: StructType,
    ...) extends RDD[InternalRow]
```

`compute(partition, context)` calls `readFunction` for each `PartitionedFile` in the partition
and chains the resulting iterators. The `readFunction` is a closure capturing the file format
reader (e.g., `ParquetFileFormat.buildReaderWithPartitionValues`).

**Locality:** `getPreferredLocations` returns HDFS block locations so tasks run close to data.

### `ShuffledRowRDD`

```
sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala
```

Reads shuffle output on the reduce side. Works with `ShufflePartitionSpec` to support AQE
partition coalescing and skew join splitting. See `docs/shuffle-and-exchanges-guide.md` for details.

### `MapPartitionsRDD`

```
core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala
```

The standard Spark RDD transformation. Almost all unary SQL operators ultimately produce one
via `child.execute().mapPartitions*`. The SQL-specific variants used:

| Method | Description |
|---|---|
| `mapPartitionsWithIndex` | Standard; passes partition index and row iterator |
| `mapPartitionsWithIndexInternal` | Like above but marked as internal; can preserve partition sizes |
| `mapPartitionsWithEvaluator` | New path (when `spark.sql.usePartitionEvaluator=true`); uses `PartitionEvaluator` interface |

### `ZippedPartitionsRDD2`

```
core/src/main/scala/org/apache/spark/rdd/ZippedPartitionsRDD.scala
```

Used by binary operators like `SortMergeJoinExec` and `WholeStageCodegenExec` (when there are
two input RDDs). Iterates two RDDs partition-by-partition simultaneously, requiring equal
partition counts.

### `UnionRDD` / `SQLPartitioningAwareUnionRDD`

Used by `UnionExec`. The SQL-specific version is `SQLPartitioningAwareUnionRDD` from
`basicPhysicalOperators.scala`, which preserves partitioning information for downstream operators.

### `EmptyRDD`

```
core/src/main/scala/org/apache/spark/rdd/EmptyRDD.scala
```

An RDD with zero partitions. Used by `EmptyRelationExec` and as the degenerate case in many places.

### `ParallelCollectionRDD`

```
core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala
```

Created by `SparkContext.parallelize(seq, numPartitions)`. Used by `LocalTableScanExec` for
small in-memory datasets and by `RangeExec` for generated integer ranges.

### `HadoopRDD` / `NewHadoopRDD`

The low-level RDD that reads from HDFS via `InputFormat`. Not used directly by Spark SQL —
`FileScanRDD` replaces it for file sources. Still used by `HiveTableScanExec` via the Hive
input format path.

---

## 7. How a Row Is Represented: InternalRow and UnsafeRow

All RDDs in Spark SQL carry `InternalRow` (not `Row` — that is the external API type).

### `InternalRow`

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala
```

Abstract base. Methods: `get(ordinal, dataType)`, `getInt(i)`, `getLong(i)`, `getString(i)`, etc.

Two key implementations:

| Class | Description |
|---|---|
| `GenericInternalRow(values: Array[Any])` | Interpreted mode; stores boxed objects |
| `UnsafeRow` | Off-heap binary format; the standard format inside exchanges and codegen |

### `UnsafeRow`

```
common/unsafe/src/main/java/org/apache/spark/unsafe/types/UnsafeRow.java
```

Binary row format backed by a contiguous byte array (on-heap or off-heap):

```
[null bitmap (8 bytes per 64 fields)] [fixed-width fields] [variable-length data]
```

- Fixed-width fields (int, long, double, etc.) stored inline.
- Variable-length fields (string, array, struct) stored as an offset+length pointer in the
  fixed section with the actual bytes at the end of the row.
- No Java object overhead; can be serialized by simply copying bytes.
- Supports in-place field updates via `setInt(i, v)`, `setLong(i, v)`, etc.

`UnsafeRow` is the format used by:
- `ShuffleExchangeExec` (via `UnsafeRowSerializer`)
- Outputs of `WholeStageCodegenExec` (via `UnsafeProjection` / `GenerateUnsafeProjection`)
- Hash aggregation buffers
- Sort keys

### `UnsafeProjection`

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/UnsafeProjection.scala
```

A `Projection[InternalRow, UnsafeRow]` that converts any `InternalRow` to `UnsafeRow`. It is
code-generated via `GenerateUnsafeProjection`. Used heavily at the boundaries of codegen stages
where interpreted rows must be converted to unsafe format.

---

## 8. Whole-Stage Code Generation (WSCG)

Whole-Stage Code Generation fuses multiple operator pipelines into a single compiled Java class
that processes rows without virtual dispatch, without materializing intermediate rows, and without
iterator overhead between operators.

### The Core Problem WSCG Solves

In interpreted mode, executing `Project(Filter(Scan))` works like this:
```
project.next() → filter.next() → scan.next() → [get row, box fields]
                                               → [unbox, evaluate predicate]
                                  → [unbox, evaluate projection]
```
Each `next()` call is a virtual dispatch. Fields are boxed/unboxed at every boundary.
This creates significant CPU overhead from branch mispredictions and JVM overhead.

With WSCG, the entire chain becomes a single tight Java loop:
```java
while (input.hasNext()) {
  InternalRow row = input.next();
  boolean isNull_1 = row.isNullAt(0);
  int value_1 = isNull_1 ? -1 : row.getInt(0);
  if (!isNull_1 && (value_1 > 10)) {   // filter
    // project: compute output expressions and append to result buffer
    append(new UnsafeRow(...));
  }
}
```
All type information is baked in at codegen time. No virtual dispatch. No boxing.

### `CollapseCodegenStages` — The Rule That Inserts WSCG

`CollapseCodegenStages` is a `Rule[SparkPlan]` in the preparations sequence. It does a
depth-first post-order traversal of the physical plan and inserts `WholeStageCodegenExec` on top
of every maximal chain of codegen-eligible operators.

```scala
case class CollapseCodegenStages(
    codegenStageCounter: AtomicInteger = new AtomicInteger(0))
  extends Rule[SparkPlan] {

  // An operator is eligible if:
  // 1. It is a CodegenSupport with supportCodegen == true
  // 2. None of its expressions use CodegenFallback
  // 3. Neither its output nor its children's output have too many nested fields
  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.exists(e => !supportCodegen(e)))
      val hasTooManyOutputFields = WholeStageCodegenExec.isTooManyFields(conf, plan.schema)
      val hasTooManyInputFields =
        plan.children.exists(p => WholeStageCodegenExec.isTooManyFields(conf, p.schema))
      !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
    case _ => false
  }

  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan: LocalTableScanExec => plan  // fast driver-local paths excluded
      case plan: EmptyRelationExec => plan
      case plan: CodegenSupport if supportCodegen(plan) =>
        WholeStageCodegenExec(insertInputAdapter(plan))(codegenStageCounter.incrementAndGet())
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageCodegen))
    }
  }

  private def insertInputAdapter(plan: SparkPlan): SparkPlan = {
    plan match {
      case p if !supportCodegen(p) =>
        InputAdapter(insertWholeStageCodegen(p))  // boundary: non-codegen subtree
      case j: SortMergeJoinExec =>
        // SMJ children each get their own WholeStageCodegen stage
        j.withNewChildren(j.children.map(c => InputAdapter(insertWholeStageCodegen(c))))
      case j: ShuffledHashJoinExec =>
        j.withNewChildren(j.children.map(c => InputAdapter(insertWholeStageCodegen(c))))
      case p =>
        p.withNewChildren(p.children.map(insertInputAdapter))
    }
  }
}
```

**Codegen stage boundaries:** Exchanges, non-codegen operators (e.g., `SortAggregateExec`),
and join children (for `SortMergeJoin`/`ShuffledHashJoin`) are wrapped in `InputAdapter`,
which becomes a leaf in the codegen tree and reads from the child plan's RDD via iterator.

**Explain output:** Codegen stages are shown with `*(stageId)` prefix:
```
== Physical Plan ==
*(3) SortMergeJoin [a#1], [b#2], Inner
:- *(1) Filter (isnotnull(a#1))
:  +- *(1) FileScan parquet ...
+- Exchange hashpartitioning(b#2, 200)
   +- *(2) Project [b#2]
      +- *(2) FileScan parquet ...
```

### `WholeStageCodegenExec`

```scala
case class WholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int)
  extends UnaryExecNode with CodegenSupport
```

The root of each codegen stage. Its `doExecute()` drives the entire codegen pipeline:

```scala
override def doExecute(): RDD[InternalRow] = {
  val (ctx, cleanedSource) = doCodeGen()   // generate Java source on the driver

  val (_, compiledCodeStats) = try {
    CodeGenerator.compile(cleanedSource)   // compile via Janino (or return cached)
  } catch {
    case NonFatal(_) if conf.codegenFallback =>
      return child.execute()               // fallback to interpreted
  }

  // Check for methods too large for JIT
  if (compiledCodeStats.maxMethodCodeSize > conf.hugeMethodLimit) {
    return child.execute()                 // fallback: JIT won't optimize huge methods
  }

  val references = ctx.references.toArray   // driver-side objects sent to executors
  val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()  // 1 or 2 input RDDs

  val evaluatorFactory = new WholeStageCodegenEvaluatorFactory(cleanedSource, durationMs, references)
  if (rdds.length == 1) {
    rdds.head.mapPartitionsWithIndex { (index, iter) =>
      val evaluator = evaluatorFactory.createEvaluator()
      evaluator.eval(index, iter)    // instantiate and run generated class
    }
  } else {
    rdds.head.zipPartitions(rdds(1)) { ... }.mapPartitionsWithIndex { ... }
  }
}
```

### `doCodeGen()` — Generating the Java Source

```scala
def doCodeGen(): (CodegenContext, CodeAndComment) = {
  val ctx = new CodegenContext
  // Triggers the produce/consume protocol across the child subtree
  val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)

  ctx.addNewFunction("processNext",
    s"""
      protected void processNext() throws java.io.IOException {
        ${code.trim}
      }
    """, inlineToOuterClass = true)

  val className = s"GeneratedIteratorForCodegenStage$codegenStageId"

  val source = s"""
    public Object generate(Object[] references) {
      return new $className(references);
    }

    final class $className extends ${classOf[BufferedRowIterator].getName} {
      private Object[] references;
      private scala.collection.Iterator[] inputs;
      ${ctx.declareMutableStates()}

      public $className(Object[] references) { this.references = references; }

      public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        ${ctx.initMutableStates()}
        ${ctx.initPartition()}
      }
      ${ctx.emitExtraCode()}
      ${ctx.declareAddedFunctions()}
    }
  """.trim
  (ctx, cleanedSource)
}
```

The generated class extends `BufferedRowIterator`. Its `processNext()` method fills an internal
buffer with `UnsafeRow` results. The iterator protocol (`.hasNext`, `.next`) drains this buffer.

### Code distribution to executors

When the generated source exceeds `spark.sql.codegen.broadcastCleanedSourceThreshold`:
```scala
private[spark] def tryBroadcastCleanedSource(code: CodeAndComment) = {
  if (enabled && exceedThreshold()) {
    Left(sparkContext.broadcast(code))   // serialize once, send to all executors
  } else {
    Right(code)                          // inline in task closure (default for small code)
  }
}
```

---

## 9. CodegenContext — The Code Generation State Machine

`CodegenContext` is the mutable accumulator that holds all state while a codegen subtree is
being processed. It is created once per `WholeStageCodegenExec` and passed to every operator
in the chain.

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala
class CodegenContext extends Logging {
  val references: mutable.ArrayBuffer[Any]  // driver-side objects (metrics, functions, etc.)
  var INPUT_ROW: String                      // name of the current InternalRow variable
  var currentVars: Seq[ExprCode]             // current column variables (for columnar access)
  var freshNamePrefix: String                // operator-specific variable name prefix
  ...
}
```

### Key APIs

#### `addReferenceObj` — Accessing Driver-Side Objects in Generated Code

Executor-side generated code cannot directly reference driver-side objects (they are not
serializable into the task closure). Instead, they are stored in `ctx.references` and accessed
via a typed cast:

```scala
def addReferenceObj(objName: String, obj: Any, className: String = null): String = {
  val idx = references.length
  references += obj
  val clsName = Option(className).getOrElse(obj.getClass.getName)
  s"(($clsName) references[$idx] /* $objName */)"
}
```

Usage pattern in operators:
```scala
val numOutputRows = metricTerm(ctx, "numOutputRows")
// numOutputRows becomes something like: "((SQLMetric) references[3] /* numOutputRows */)"
// This compiles to: references[3].add(1)
```

#### `addMutableState` — Instance Variables in the Generated Class

```scala
def addMutableState(
    javaType: String,
    variableName: String,
    initFunc: String => String = _ => "",
    useFreshName: Boolean = true,
    forceInline: Boolean = false): String
```

Declares a field in the generated class body. Initialization code goes in `init()`. For
example, a `scala.collection.Iterator` field for reading input:

```scala
val input = ctx.addMutableState("scala.collection.Iterator", "input",
  v => s"$v = inputs[0];", forceInline = true)
// Generates: private scala.collection.Iterator input;  // in field declarations
//            input = inputs[0];                        // in init()
```

#### `addNewFunction` — Helper Methods in the Generated Class

Long generated code is split into helper methods to avoid hitting the 64KB JVM method size limit:

```scala
def addNewFunction(funcName: String, funcCode: String, inlineToOuterClass: Boolean = false): String
```

When a method would exceed the limit, it is placed in a new inner class to avoid the restriction.

#### `freshName` — Unique Variable Names

```scala
def freshName(name: String): String
```

Generates `{freshNamePrefix}_{name}_{counter}` to avoid variable name collisions when multiple
operators in the same stage generate code for the same expression.

#### `subexpressionEliminationForWholeStageCodegen` — CSE

Identifies common sub-expressions shared across multiple column evaluations and hoists their
computation into shared variables. For example, if `a + b` appears in both `Project` and a
downstream `Filter`, it is computed once.

---

## 10. ExprCode — Code for One Expression Evaluation

`ExprCode` is a triple representing the generated code for evaluating one `Expression`:

```scala
case class ExprCode(
    var code: Block,      // statements to evaluate the expression (empty if already evaluated)
    var isNull: ExprValue, // Java expression that evaluates to boolean: is this value null?
    var value: ExprValue   // Java expression that gives the expression's value (invalid if null)
)
```

Factory methods:
```scala
ExprCode.forNonNullValue(value) // isNull = false literal, no code
ExprCode.forNullValue(dataType) // isNull = true literal, value = default
```

### How Expressions Generate Code

Every `Expression` subclass implements:
```scala
def genCode(ctx: CodegenContext): ExprCode
```

For example, a literal integer `42` generates:
```scala
ExprCode(code = EmptyBlock, isNull = FalseLiteral, value = JavaCode.literal("42", IntegerType))
```

A column reference `a` (bound to position 0, IntegerType, nullable) generates:
```java
boolean isNull_a = i.isNullAt(0);        // code block
int value_a = isNull_a ? -1 : i.getInt(0);
// ExprCode(code = above, isNull = "isNull_a", value = "value_a")
```

An arithmetic expression `a + b` (both IntegerType) generates:
```java
boolean isNull_result = isNull_a || isNull_b;
int value_result = isNull_result ? -1 : (value_a + value_b);
```

The `code` block is emitted exactly once, then cleared (`ev.code = EmptyBlock`) to prevent
double evaluation if the same `ExprCode` is referenced multiple times.

---

## 11. Operator-Level Codegen: Produce/Consume Protocol

The codegen protocol between operators is a **push-based** model: a child operator
**produces** rows and **pushes** them to its parent via `consume()`. This is the opposite of
the pull-based iterator model (`next()`).

### The Protocol

```
WholeStageCodegenExec calls: child.produce(ctx, this)
  ↓
Each operator implements: doProduce(ctx)   — generates the row-producing loop
                           doConsume(ctx)  — generates what to do with each produced row
  ↓
child.produce() → child.doProduce() → ... calls child's child.produce() ...
  eventually calls: consume(ctx, outputVars) → parent.doConsume(ctx, inputVars, rowVar)

The chain builds up a single string of Java code from the inside out.
```

### `produce(ctx, parent)` and `doProduce(ctx)`

`produce` is the entry point:
```scala
final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
  this.parent = parent
  ctx.freshNamePrefix = variablePrefix
  s"""
     |${ctx.registerComment(s"PRODUCE: ${this.simpleString(...)}")}
     |${doProduce(ctx)}
   """.stripMargin
}
```

`doProduce` generates the code for the producer's outer loop or block. Leaf nodes like
`InputRDDCodegen` (used by `InputAdapter`) generate:
```java
while (limitNotReached && input.hasNext()) {
  InternalRow row = (InternalRow) input.next();
  // ... consume(ctx, null, row) generates parent.doConsume() code here ...
  if (shouldStop()) return;
}
```

### `consume(ctx, outputVars, row)` and `doConsume(ctx, input, row)`

`consume` is the transition from child to parent:
```scala
final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
  // Set up currentVars and INPUT_ROW for the parent to use
  ctx.currentVars = inputVars
  ctx.INPUT_ROW = null
  ctx.freshNamePrefix = parent.variablePrefix

  // Optionally split into a separate method to prevent huge method bodies
  parent.doConsume(ctx, inputVars, rowVar)
}
```

`doConsume` generates the code that processes each incoming row. It uses `outputVars` (column
variables already evaluated by the child) and optionally calls `consume(ctx, resultVars)` to
pass results to its own parent.

### Call Graph Example (`WholeStageCodegen → Filter → Scan`)

```
WholeStageCodegen
  ├── calls:  scan.produce(ctx, filter)     // ask scan to generate its loop
  │             → scan.doProduce(ctx)
  │                 → while (input.hasNext()) {
  │                     InternalRow row = input.next();
  │                     → scan.consume(ctx, null, "row")
  │                          → filter.doConsume(ctx, ...)    // filter's code
  │                               if (condition) {
  │                                 → filter.consume(ctx, resultVars)
  │                                      → wholeStage.doConsume(ctx, ...) // append to buffer
  │                               }
  │                 }
```

The resulting Java string is a single nested code block — no function calls at runtime.

### `WholeStageCodegenExec.doConsume`

The terminal consumer at the top of the chain:
```scala
override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
  val doCopy = if (needCopyResult) ".copy()" else ""
  s"""
    |${row.code}
    |append(${row.value}$doCopy);
   """.stripMargin.trim
}
```

`append()` adds the row to the `BufferedRowIterator`'s internal output buffer. The `.copy()`
is needed when an operator (e.g., join) may produce multiple output rows from the same input row.

---

## 12. Concrete Codegen Examples

### `FilterExec.doConsume`

```scala
override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
  val numOutput = metricTerm(ctx, "numOutputRows")
  val predicateCode = generatePredicateCode(ctx, child.output, input, ...)

  // Resets isNull to false for IsNotNull-filtered columns (avoids dead branches downstream)
  val resultVars = input.zipWithIndex.map { case (ev, i) =>
    if (notNullAttributes.contains(child.output(i).exprId)) ev.isNull = FalseLiteral
    ev
  }

  // Wrap in do-while so generated checks can jump out with "continue;"
  s"""
     |do {
     |  $predicateCode          // if (!pred) break;
     |  $numOutput.add(1);
     |  ${consume(ctx, resultVars)}  // pass to parent
     |} while (false);
   """.stripMargin
}
```

Generated code (for `WHERE a > 10`):
```java
do {
  boolean isNull_a = i.isNullAt(0);
  int value_a = isNull_a ? -1 : i.getInt(0);
  if (isNull_a || !(value_a > 10)) continue;  // predicate fails → skip row
  numOutputRows.add(1);
  // parent.doConsume generated here...
} while (false);
```

### `ProjectExec.doConsume`

```scala
override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
  val exprs = bindReferences(projectList, child.output)
  val resultVars = exprs.map(_.genCode(ctx))

  s"""
     |${subExprsCode}
     |${consume(ctx, resultVars)}
   """.stripMargin
}
```

Generated code (for `SELECT a + 1 AS b`):
```java
// evaluate projection expression
boolean isNull_b = isNull_a;
int value_b = isNull_b ? -1 : (value_a + 1);
// pass b to parent
```

### `InputRDDCodegen.doProduce` (the loop in `InputAdapter`)

```scala
override def doProduce(ctx: CodegenContext): String = {
  val input = ctx.addMutableState("scala.collection.Iterator", "input",
    v => s"$v = inputs[0];", forceInline = true)
  val row = ctx.freshName("row")
  s"""
     | while ($limitNotReachedCond $input.hasNext()) {
     |   InternalRow $row = (InternalRow) $input.next();
     |   ${consume(ctx, null, row)}
     |   ${shouldStopCheckCode}
     | }
   """.stripMargin
}
```

---

## 13. Janino Compilation and Code Cache

### Janino

Spark uses the **Janino** embedded Java compiler (not the standard JDK `javac`) to compile
generated code at runtime. Janino is fast and lightweight but does not support all Java language
features (no generics reflection, limited annotation support).

```scala
// CodeGenerator.doCompile
private[this] def doCompile(code: CodeAndComment): (GeneratedClass, ByteCodeStats) = {
  val evaluator = new ClassBodyEvaluator()
  evaluator.setParentClassLoader(new ParentClassLoader(Utils.getContextOrSparkClassLoader))
  evaluator.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
  evaluator.setDefaultImports(
    classOf[Platform].getName,
    classOf[InternalRow].getName,
    classOf[UnsafeRow].getName,
    // ... more imports
  )
  evaluator.setExtendedClass(classOf[GeneratedClass])
  evaluator.cook("generated.java", code.body)  // compile
  (evaluator.getClazz().getConstructor().newInstance().asInstanceOf[GeneratedClass], codeStats)
}
```

The compiled class must implement `GeneratedClass`:
```java
// The factory method called to produce a new BufferedRowIterator
public abstract Object generate(Object[] references);
```

Janino compiles directly to JVM bytecode (no intermediate `.class` file). The resulting class
is loaded by the task's `ClassLoader` on the executor.

### Code Cache

Compilation is expensive (~5–50ms). The cache avoids recompiling the same code:

```scala
private val cache = {
  // Keyed by (classloader weak reference, CodeAndComment)
  // Value: (GeneratedClass, ByteCodeStats)
  NonFateSharingCache(loadFunc, SQLConf.get.codegenCacheMaxEntries)
}
```

`NonFateSharingCache` is a Guava `LoadingCache` variant where cache misses do NOT block other
threads — each cache miss compiles independently. This prevents one slow compilation from
blocking all threads.

The cache key is `(WeakReference(classloader), CodeAndComment)`. Two queries that produce
identical generated code share the same compiled class. The `WeakReference` prevents memory
leaks when the classloader is GC'd.

**Cache size:** Controlled by `spark.sql.codegen.cache.maxEntries` (default 100). When the cache
is full, the least-recently-used entry is evicted, causing recompilation on the next access.

### ByteCodeStats and JIT Interaction

After compilation, Spark checks whether any method exceeds `spark.sql.codegen.hugeMethodLimit`
(default 65,535 bytes). Methods larger than this cannot be JIT-compiled by HotSpot (the 64KB
bytecode limit). If a method is too large:

```scala
if (compiledCodeStats.maxMethodCodeSize > conf.hugeMethodLimit) {
  logInfo(s"Found too long generated codes and JIT optimization might not work...")
  return child.execute()  // fall back to interpreted execution
}
```

The `splitConsumeFuncByOperator` optimization (controlled by
`spark.sql.codegen.splitConsumeFuncByOperator`) proactively splits `doConsume` bodies into
separate methods to stay below this limit.

---

## 14. Codegen Fallbacks and Safety Valves

### When Codegen Is Disabled Entirely

1. `spark.sql.codegen.wholeStage = false` — disables WSCG globally
2. `spark.sql.codegen.factoryMode = NO_CODEGEN` — disables all codegen
3. Too many nested fields (`> spark.sql.codegen.maxFields` = 100) — `CollapseCodegenStages` skips the subtree

### Expression-Level Fallback: `CodegenFallback`

Any `Expression` that cannot generate code implements `CodegenFallback`:
```scala
trait CodegenFallback extends Expression {
  override def genCode(ctx: CodegenContext): ExprCode = {
    // Add a reference to this expression object
    val objectTerm = ctx.addReferenceObj("nativeExpr", this, classOf[Expression].getName)
    val placeHolder = ctx.registerComment(s"$nodeName: evaluate via interpreted mode")
    // Generate code that calls InternalRow.apply() on the whole row
    ...
  }
}
```

When any expression in an operator uses `CodegenFallback`, `CollapseCodegenStages.supportCodegen`
returns `false` for that operator and it is wrapped in `InputAdapter`.

### Operator-Level Fallback

Some operators set `supportCodegen = false` explicitly:
- `SortAggregateExec` (only `HashAggregateExec` supports codegen)
- `ObjectHashAggregateExec`
- Operators with `ObjectType` output (Python UDFs, etc.)
- `BroadcastExchangeExec`, `ShuffleExchangeExec` (they are stage boundaries)

### Compile-Time Fallback

`WholeStageCodegenExec.doExecute()` catches `NonFatal` compilation errors and falls back to
`child.execute()` when `spark.sql.codegen.fallback = true`:
```scala
try {
  CodeGenerator.compile(cleanedSource)
} catch {
  case NonFatal(_) if !Utils.isTesting && conf.codegenFallback =>
    return child.execute()
}
```

---

## 15. End-to-End Example: SELECT with Filter and Project

Query: `SELECT id * 2 AS result FROM range(1000000) WHERE id % 3 = 0`

### Step 1: Logical Plan

```
Project [id * 2 AS result]
└─ Filter [(id % 3) = 0]
   └─ Range(0, 1000000, step=1)
```

### Step 2: Physical Plan (after SparkPlanner)

```
ProjectExec [id * 2 AS result]
└─ FilterExec [(id % 3) = 0]
   └─ RangeExec(0, 1000000, step=1, numSlices=8)
```

### Step 3: After `CollapseCodegenStages`

```
WholeStageCodegenExec (stage 1)
└─ ProjectExec [id * 2 AS result]
   └─ FilterExec [(id % 3) = 0]
      └─ RangeExec(0, 1000000, step=1, numSlices=8)
```

All three operators support codegen → collapsed into one stage.

### Step 4: Code Generation (produce/consume flow)

`WholeStageCodegenExec` calls `project.produce(ctx, this)`:
1. `ProjectExec.doProduce` → delegates to `filter.produce(ctx, project)`
2. `FilterExec.doProduce` → delegates to `range.produce(ctx, filter)`
3. `RangeExec.doProduce` → generates the outer loop:
   ```java
   while (count < end) {
     long value_id = count;
     count += step;
     // filter.doConsume:
     boolean isNull_mod = false;
     long value_mod = value_id % 3L;
     if (value_mod != 0L) continue;
     // project.doConsume:
     long value_result = value_id * 2L;
     // wholeStage.doConsume:
     append(unsafeRowWithResult);
   }
   ```

### Step 5: RDD DAG

```
WholeStageCodegenExec.doExecute():
  inputRDDs = [RangeExec.execute() → ParallelCollectionRDD]
  → ParallelCollectionRDD.mapPartitionsWithIndex { (i, iter) =>
       val evaluator = new WholeStageCodegenEvaluatorFactory(...)
       evaluator.eval(i, iter)   // runs GeneratedIteratorForCodegenStage1
    }
  → MapPartitionsRDD[InternalRow]
```

### Step 6: Action Triggers Execution

When `.collect()` is called:
1. Spark DAGScheduler splits the RDD lineage into stages.
2. Task serialization: `GeneratedIteratorForCodegenStage1` class is compiled once and either
   inlined in the task closure or broadcast to executors.
3. Each executor task instantiates the generated class, calls `init(partitionIndex, inputs)`,
   then iterates via `hasNext()/next()` until all rows are produced.
4. Results flow back to the driver as `Array[InternalRow]`.

---

## 16. Key Configuration Properties

| Property | Default | Effect |
|---|---|---|
| `spark.sql.codegen.wholeStage` | true | Enable/disable Whole-Stage Code Generation |
| `spark.sql.codegen.maxFields` | 100 | Max nested fields before codegen is skipped |
| `spark.sql.codegen.hugeMethodLimit` | 65535 | Bytecode size limit; methods above this fall back |
| `spark.sql.codegen.fallback` | true | Fall back to interpreted if codegen fails |
| `spark.sql.codegen.logging.maxLines` | 1000 | Max lines to log when dumping generated code |
| `spark.sql.codegen.useIdInClassName` | true | Include stage ID in generated class names |
| `spark.sql.codegen.splitConsumeFuncByOperator` | true | Split doConsume into sub-methods to avoid huge methods |
| `spark.sql.codegen.cache.maxEntries` | 100 | Max number of compiled classes to cache |
| `spark.sql.subexpressionElimination.enabled` | true | Enable CSE in codegen |
| `spark.sql.codegen.broadcastCleanedSourceThreshold` | 16K | Source size above which code is broadcast (not inlined) |
| `spark.sql.usePartitionEvaluator` | false | Use `PartitionEvaluator` instead of `mapPartitionsWithIndex` |

---

## 17. Key Source Files

| File | Purpose |
|---|---|
| `sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala` | Orchestrator: all lazy phases, `preparations`, `toRdd` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala` | Abstract physical plan; `execute()`, `executeQuery()`, `prepare()` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenExec.scala` | `CodegenSupport` trait, `InputAdapter`, `WholeStageCodegenExec`, `CollapseCodegenStages` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/basicPhysicalOperators.scala` | `ProjectExec`, `FilterExec`, `SortExec`, `UnionExec` with codegen implementations |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala` | `CodegenContext`, `ExprCode`, `CodeGenerator.compile()`, Janino integration, code cache |
| `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/GenerateUnsafeProjection.scala` | Codegen for `InternalRow → UnsafeRow` conversion |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala` | RDD for file-based scans |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala` | RDD for shuffle reads |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SQLExecutionRDD.scala` | Wrapper propagating `SQLConf` to executor threads |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ExistingRDD.scala` | `RDDScanExec`, `LocalTableScanExec` |
| `common/unsafe/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java` | Binary row format |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenEvaluatorFactory.scala` | Factory that instantiates and runs a generated `BufferedRowIterator` |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala` | Hash aggregate with codegen |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinExec.scala` | Sort-merge join with codegen |
