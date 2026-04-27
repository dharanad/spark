# Spark Catalyst Optimizer Rule Developer Guide

A practical reference for writing new `Rule[LogicalPlan]` optimizer rules in Spark Catalyst.

---

## Table of Contents

1. [Rule Infrastructure](#1-rule-infrastructure)
2. [Anatomy of a LogicalPlan](#2-anatomy-of-a-logicalplan)
3. [Expression Hierarchy](#3-expression-hierarchy)
4. [Tree Traversal and Transformation API](#4-tree-traversal-and-transformation-api)
5. [Common Helper Traits](#5-common-helper-traits)
6. [Writing a New Rule — Patterns and Recipes](#6-writing-a-new-rule--patterns-and-recipes)
7. [Registering the Rule in Optimizer.scala](#7-registering-the-rule-in-optimizerscala)
8. [Testing](#8-testing)
9. [Expression vs Attribute](#9-expression-vs-attribute)
10. [Converting an Expression to an Attribute](#10-converting-an-expression-to-an-attribute)

---

## 1. Rule Infrastructure

### 1.1 `Rule[LogicalPlan]` — the base

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/Rule.scala
abstract class Rule[TreeType <: TreeNode[_]] extends SQLConfHelper with Logging {
  protected lazy val ruleId: RuleId = RuleIdCollection.getRuleId(this.ruleName)
  val ruleName: String = { /* derived from class/object name */ }
  def apply(plan: TreeType): TreeType   // the only required method
}
```

Key points:
- Implement `apply`: receive a plan, return a (possibly rewritten) plan.
- Return the **same object** (`fastEquals` true) when nothing changed — the executor uses this to detect fixed-point convergence and to skip future traversals via `ruleId`.
- `conf` from `SQLConfHelper` gives access to `SQLConf` settings (`conf.constraintPropagationEnabled`, etc.).
- Rules are **stateless**; prefer `object` over `class`. If mutable state is required (e.g. parameterised behaviour), use a `class` but make sure no state leaks between plan applications.

### 1.2 `RuleExecutor[LogicalPlan]` — the engine

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/RuleExecutor.scala
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {
  protected def batches: Seq[Batch]
  def execute(plan: TreeType): TreeType
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType
}
```

**Strategies**

| Strategy | Semantics |
|---|---|
| `Once` | Each rule runs exactly once per batch. In test mode an idempotence check is enforced. |
| `FixedPoint(n)` | Repeats the whole batch until no rule changes the plan (fixed point) or `n` iterations reached, whichever comes first. `FixedPoint(1)` is functionally equivalent to `Once` but without the idempotence check. |

**Execution loop (simplified)**

```
for each Batch:
  repeat until fixed-point or maxIterations:
    for each Rule in batch:
      result = rule(curPlan)
      if !result.fastEquals(curPlan):
        curPlan = result   // rule was effective
```

Rules within a batch observe changes made by earlier rules in the same iteration.

**Batch definition**

```scala
protected[catalyst] case class Batch(
  name: String,
  strategy: Strategy,
  rules: Rule[TreeType]*)
```

### 1.3 `Optimizer` — the standard optimizer

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala
abstract class Optimizer(catalogManager: CatalogManager)
  extends RuleExecutor[LogicalPlan] with SQLConfHelper
```

**Key overridable hooks (for extending Spark's optimizer)**

| Hook | Purpose |
|---|---|
| `extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]]` | Appended to the large "Operator Optimization" FixedPoint batch. Best place for most new rules. |
| `earlyScanPushDownRules: Seq[Rule[LogicalPlan]]` | Runs in `"Early Filter and Projection Push-Down"` (Once). For scan-level predicates/projections. |
| `preCBORules: Seq[Rule[LogicalPlan]]` | Runs before cost-based join reorder. |

**Batch ordering (abbreviated)**

```
Finish Analysis             FixedPoint(1)
Rewrite With expression     FixedPoint
Eliminate Distinct          Once
Inline CTE                  Once
Union                       FixedPoint
LocalRelation early         FixedPoint
Pullup Correlated Exprs     Once
Subquery                    FixedPoint(1)
Replace Operators           FixedPoint
Aggregate                   FixedPoint
Operator Optimization       FixedPoint   ← most rules live here
  + Infer Filters (Once) interleaved
Push extra predicate        FixedPoint
Pre CBO Rules               Once
Early Filter/Proj Push-Down Once
Join Reorder                FixedPoint(1)
Eliminate Sorts             Once
Decimal Optimizations       FixedPoint
Distinct Aggregate Rewrite  Once
LocalRelation               FixedPoint
RewriteSubquery             Once
NormalizeFloatingNumbers    Once
```

**Rule exclusion**

Users can disable rules via `spark.sql.optimizer.excludedRules`. Override `nonExcludableRules` to prevent a critical rule from being excluded.

---

## 2. Anatomy of a LogicalPlan

### 2.1 Class hierarchy

```
TreeNode[LogicalPlan]
  └─ QueryPlan[LogicalPlan]        (expressions, output, stats)
       └─ LogicalPlan              (resolved, constraints, isStreaming)
            ├─ LeafNode            (no children, implements computeStats)
            ├─ UnaryNode           (one child)
            │   └─ OrderPreservingUnaryNode  (preserves child ordering)
            └─ BinaryNode          (two children: left, right)
```

Mixed-in traits on `LogicalPlan`:
- `AnalysisHelper` — tracks resolution state
- `LogicalPlanStats` — lazy `stats: Statistics` with row count / size estimates
- `LogicalPlanDistinctKeys` — propagates distinct key information
- `QueryPlanConstraints` — infers invariant predicates (`constraints`)

### 2.2 Key fields and methods

| Field / Method | Type | Description |
|---|---|---|
| `output` | `Seq[Attribute]` | **Abstract.** Columns produced by this operator, in order. |
| `outputSet` | `AttributeSet` | Cached set view of `output`. Use for O(1) membership tests. |
| `children` | `Seq[LogicalPlan]` | Immutable child operators. |
| `inputSet` | `AttributeSet` | Union of all children's `outputSet`. |
| `references` | `AttributeSet` | Attributes referenced by expressions in this node (excluding children). |
| `producedAttributes` | `AttributeSet` | Attributes this node introduces (e.g. Alias exprIds). Default: empty. |
| `missingInput` | `AttributeSet` | `references -- inputSet -- producedAttributes`: attributes this node needs but children don't provide. Non-empty means a resolution bug. |
| `resolved` | `Boolean` (lazy) | True when all expressions are resolved and `missingInput` is empty. |
| `childrenResolved` | `Boolean` | True when all children are resolved. |
| `constraints` | `ExpressionSet` (lazy) | Inferred invariants on output rows (e.g. `a > 0`, `b IS NOT NULL`). |
| `stats` | `Statistics` | Row count / byte size estimates. Do not call inside a rule hot path; it triggers subtree traversal. |
| `isStreaming` | `Boolean` (lazy) | True if any leaf is a streaming source. Many rules guard against transforming streaming subtrees. |
| `maxRows` | `Option[Long]` | Upper bound on row count. Used to eliminate redundant limits. |
| `maxRowsPerPartition` | `Option[Long]` | Per-partition row bound. |
| `outputOrdering` | `Seq[SortOrder]` | Guarantees on output row order. Used by `RemoveRedundantSorts`. |

### 2.3 Key concrete operators

All are case classes in `basicLogicalOperators.scala` unless noted.

**Project**
```scala
case class Project(projectList: Seq[NamedExpression], child: LogicalPlan)
// output = projectList.map(_.toAttribute)
```
Implements `SELECT` expressions. `projectList` must not be empty.

**Filter**
```scala
case class Filter(condition: Expression, child: LogicalPlan)
// output = child.output
// validConstraints = child.constraints ++ splitConjunctivePredicates(condition)
```

**Join**
```scala
case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,   // Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti, Cross
  condition: Option[Expression],
  hint: JoinHint)
// output = computed by Join.computeOutput(joinType, left.output, right.output)
```

**Aggregate**
```scala
case class Aggregate(
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression],
  child: LogicalPlan,
  hint: Option[AggregateHint] = None)
// output = aggregateExpressions.map(_.toAttribute)
// maxRows = Some(1) if groupingExpressions.isEmpty
```

**Window**
```scala
case class Window(
  windowExpressions: Seq[NamedExpression],
  partitionSpec: Seq[Expression],
  orderSpec: Seq[SortOrder],
  child: LogicalPlan,
  hint: Option[WindowHint] = None)
// output = child.output ++ windowExpressions.map(_.toAttribute)
```

**Sort**
```scala
case class Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan,
  hint: Option[SortHint] = None)
// output = child.output
// outputOrdering = order
// global=false means per-partition sort only
```

**GlobalLimit / LocalLimit**
```scala
case class GlobalLimit(limitExpr: Expression, child: LogicalPlan)
case class LocalLimit(limitExpr: Expression, child: LogicalPlan)
// limitExpr is always an IntegerLiteral in practice
```

**Union**
```scala
case class Union(children: Seq[LogicalPlan], byName: Boolean = false,
  allowMissingCol: Boolean = false)
// output = Union.mergeChildOutputs(children.map(_.output))
```

**SubqueryAlias**
```scala
case class SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan)
// output = child.output with qualifier updated
// canonicalized -> child.canonicalized (alias is ignored for semantic equality)
```

**LocalRelation** — in-memory data, common in tests
```scala
case class LocalRelation(output: Seq[Attribute], data: Seq[InternalRow] = Nil, ...)
```

---

## 3. Expression Hierarchy

### 3.1 Base class

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Expression.scala
abstract class Expression extends TreeNode[Expression] {
  def dataType: DataType          // abstract — result type
  def nullable: Boolean           // abstract — can this return null?

  def foldable: Boolean = false   // can be evaluated at plan time (constant sub-expressions)
  def deterministic: Boolean = ...  // true if same input always yields same output
  def resolved: Boolean = ...     // true after analysis succeeds
  def references: AttributeSet    // attributes this expression reads

  def eval(input: InternalRow = null): Any   // interpreted execution
  def genCode(ctx: CodegenContext): ExprCode // code-gen entry point
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode  // implement for new exprs

  def withNewChildren(newChildren: Seq[Expression]): Expression
  lazy val canonicalized: Expression   // normalised form for semantic equality
}
```

### 3.2 Structural subtypes

| Base | Children | Helpers provided |
|---|---|---|
| `LeafExpression` | 0 | — |
| `UnaryExpression` | 1 | `nullSafeEval(value)`, `defineCodeGen`, `nullSafeCodeGen` |
| `BinaryExpression` | 2 | `nullSafeEval(v1, v2)`, `defineCodeGen`, `nullSafeCodeGen` |
| `BinaryOperator` | 2 | Same as Binary + requires same type on both sides; `symbol: String` |
| `TernaryExpression` | 3 | `nullSafeEval(v1,v2,v3)` |

### 3.3 Important expression traits

**`Unevaluable`** — expressions that must be replaced before evaluation (e.g. `Star`, unresolved references). `eval()` and `doGenCode()` throw.

**`RuntimeReplaceable`** — has a `replacement: Expression` field; the optimizer replaces the expression with its replacement. Use for SQL functions that compile to simpler primitives (e.g. `nvl` → `coalesce`).

**`Nondeterministic`** — `rand()`, `uuid()`, etc. `deterministic = false`, `foldable = false`. Must call `initialize(partitionIndex)` before evaluation.

**`ConditionalExpression`** — `alwaysEvaluatedInputs` + `branchGroups` allow optimizers to reason about which branches are always vs. conditionally evaluated.

**`CommutativeExpression`** — `And`, `Or`, `Add`, `Multiply`, etc. `canonicalized` reorders operands by hash to enable semantic equality across e.g. `a+b` and `b+a`.

### 3.4 Named expressions, attributes, and ExprId

**`NamedExpression`** — adds `name: String`, `exprId: ExprId`, `qualifier: Seq[String]`, `toAttribute`.

**`ExprId`** — `case class ExprId(id: Long, jvmId: UUID)`. Every `Attribute` and `Alias` has a unique `ExprId`. This is the identity key used throughout the plan tree — **never reuse an ExprId across different logical attributes**.

```scala
val newId: ExprId = NamedExpression.newExprId   // thread-safe auto-increment
```

**`AttributeReference`** — a reference to a named column.
```scala
case class AttributeReference(name: String, dataType: DataType,
    nullable: Boolean = true, metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty)
```
Two `AttributeReference`s with the same `exprId` refer to the same logical column regardless of name, type, or qualifier.

**`Alias`** — wraps an expression and gives it a name + new `exprId`.
```scala
case class Alias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId, ...)
```
`alias.toAttribute` returns an `AttributeReference` with the same `exprId`.

**`AttributeSet`** — a `Set[Attribute]` where equality is based on `exprId` only.
```scala
val s = AttributeSet(plan.output)
s.contains(attr)        // O(1), ignores name/type
s ++ other              // union
s -- other              // difference
s.subsetOf(other)
```

---

## 4. Tree Traversal and Transformation API

### 4.1 Transforming the plan tree

All methods are on `TreeNode` / `LogicalPlan` and return a **new tree** (immutable). If the partial function does not match a node the node is returned unchanged.

#### Pre-order (top-down)
```scala
plan.transformDown(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan
plan.transformDownWithPruning(
  cond: TreePatternBits => Boolean,
  ruleId: RuleId = UnknownRuleId)(rule: PartialFunction[...])
```
The current node is matched first; then children are recursively transformed.  
Use when parent context determines child transformation.

#### Post-order (bottom-up)
```scala
plan.transformUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan
plan.transformUpWithPruning(cond, ruleId)(rule)
```
Children are recursively transformed first; then the current node is matched.  
Use when you need the transformed children before deciding on the parent.

#### `transform` (alias for `transformDown`)
```scala
plan.transform(rule)   // same as transformDown
```

#### `transformWithPruning` (alias for `transformDownWithPruning`)
```scala
plan.transformWithPruning(cond, ruleId)(rule)
```
This is the **recommended default** — pre-order with pruning.

#### `mapChildren`
```scala
plan.mapChildren(f: LogicalPlan => LogicalPlan): LogicalPlan
```
Applies `f` to each direct child only (not the current node, not grandchildren). Returns a new node with updated children.

#### `withNewChildren`
```scala
plan.withNewChildren(newChildren: Seq[LogicalPlan]): LogicalPlan
```
Returns a copy of the current node with children replaced. Children must be provided in the same order as `plan.children`. Used when you already know the new children.

### 4.2 Transforming expressions within a plan node

These methods transform only the expressions held by the **current plan node**, without recursing into child plans.

```scala
// Transform expressions in this plan node (pre-order inside each expression)
plan.transformExpressions(rule: PartialFunction[Expression, Expression]): plan.type
plan.transformExpressionsDown(rule)
plan.transformExpressionsWithPruning(cond, ruleId)(rule)
plan.transformExpressionsDownWithPruning(cond, ruleId)(rule)

// Post-order inside each expression
plan.transformExpressionsUp(rule)
plan.transformExpressionsUpWithPruning(cond, ruleId)(rule)

// Apply a function uniformly to each top-level expression slot
plan.mapExpressions(f: Expression => Expression): plan.type
```

To also recurse into subquery expressions (scalar subqueries, correlated predicates), use `transformAllExpressions` / `transformAllExpressionsWithPruning` (defined on `QueryPlan`).

### 4.3 Tree pattern pruning

The `WithPruning` variants accept a pruning predicate of type `TreePatternBits => Boolean`. Each node caches a bitmap of all `TreePattern` values present in its subtree. If the predicate returns false the entire subtree is skipped.

```scala
import org.apache.spark.sql.catalyst.trees.TreePattern._

// Skip subtrees that don't contain a FILTER node
plan.transformWithPruning(_.containsPattern(FILTER), ruleId) { ... }

// Skip subtrees lacking BOTH LITERAL and BINARY_COMPARISON
plan.transformWithPruning(_.containsAllPatterns(LITERAL, BINARY_COMPARISON), ruleId) { ... }

// Skip subtrees that have neither NULL_LITERAL nor TRUE_OR_FALSE_LITERAL
plan.transformWithPruning(_.containsAnyPattern(NULL_LITERAL, TRUE_OR_FALSE_LITERAL), ruleId) { ... }
```

Use `AlwaysProcess.fn` when your rule has no useful pruning predicate.

**Always prefer `WithPruning` variants in production rules.** For large plans the savings are significant.

#### Common `TreePattern` values

Expression patterns: `ALIAS`, `AND`, `ATTRIBUTE_REFERENCE`, `BINARY_COMPARISON`, `CASE_WHEN`, `CAST`, `IF`, `IN`, `INSET`, `LITERAL`, `NOT`, `NULL_LITERAL`, `OR`, `PYTHON_UDF`, `RUNTIME_REPLACEABLE`, `SCALA_UDF`, `TRUE_OR_FALSE_LITERAL`, `EXPRESSION_WITH_RANDOM_SEED`, ...

Operator patterns: `AGGREGATE`, `FILTER`, `GENERATE`, `INNER_LIKE_JOIN`, `JOIN`, `LEFT_SEMI_OR_ANTI_JOIN`, `LIMIT`, `LOCAL_RELATION`, `OUTER_JOIN`, `PROJECT`, `SORT`, `SUBQUERY_ALIAS`, `UNION`, `WINDOW`, ...

Full list in `TreePatterns.scala`.

### 4.4 Collection / search

```scala
plan.collect[B](pf: PartialFunction[LogicalPlan, B]): Seq[B]   // pre-order
plan.collectFirst[B](pf: PartialFunction[LogicalPlan, B]): Option[B]
plan.collectLeaves(): Seq[LogicalPlan]
plan.find(f: LogicalPlan => Boolean): Option[LogicalPlan]
plan.exists(f: LogicalPlan => Boolean): Boolean
plan.foreach(f: LogicalPlan => Unit): Unit      // pre-order
plan.foreachUp(f: LogicalPlan => Unit): Unit    // post-order
```

### 4.5 Equality and canonicalization

```scala
plan.fastEquals(other: TreeNode[_]): Boolean
// Returns true fast when both are the same JVM object; falls back to equals().

plan.canonicalized: LogicalPlan
// Lazy: normalises exprIds sequentially (0, 1, 2, ...) and removes cosmetic
// differences (alias names, qualifier, commutative reordering).

plan.sameResult(other: LogicalPlan): Boolean   // plan.canonicalized == other.canonicalized
plan.semanticHash(): Int                        // canonicalized.hashCode
```

### 4.6 `TreeNodeTag` — attaching metadata to a node

Used to mark a node as "already processed" (prevents infinite reapplication) or to carry user-specified hints.

```scala
val myTag: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("my.rule.processed")

// write
node.setTagValue(myTag, true)

// read
node.getTagValue(myTag): Option[Boolean]

// check presence
node.containsTag(myTag): Boolean
```

Tags are **not** copied by `copy()` or `withNewChildren()`; they live on a specific plan instance.

---

## 5. Common Helper Traits

Mix these into your rule class or object when needed.

### 5.1 `PredicateHelper`

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/predicates.scala
trait PredicateHelper extends AliasHelper with Logging
```

| Method | Description |
|---|---|
| `splitConjunctivePredicates(cond: Expression): Seq[Expression]` | Flattens `a AND b AND c` → `[a, b, c]` |
| `splitDisjunctivePredicates(cond: Expression): Seq[Expression]` | Flattens `a OR b OR c` → `[a, b, c]` |
| `buildBalancedPredicate(exprs: Seq[Expression], op: (Expression, Expression) => Expression): Expression` | Builds a balanced binary tree from a list of expressions |
| `canEvaluate(expr: Expression, plan: LogicalPlan): Boolean` | True if `expr.references ⊆ plan.outputSet` |
| `canEvaluateWithinJoin(expr: Expression): Boolean` | True if expression is safe to evaluate in a join condition |
| `extractPredicatesWithinOutputSet(cond: Expression, outputSet: AttributeSet): Option[Expression]` | Returns the sub-predicate that can be evaluated using only `outputSet` |
| `findExpressionAndTrackLineageDown(expr: Expression, plan: LogicalPlan): Option[(Expression, LogicalPlan)]` | Traces `expr` through `Project`/`Aggregate` aliases down to a leaf |

### 5.2 `AliasHelper`

```scala
// sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/AliasHelper.scala
trait AliasHelper
```

| Method | Description |
|---|---|
| `getAliasMap(plan: Project): AttributeMap[Alias]` | Maps each output attribute to the `Alias` that defines it |
| `getAliasMap(plan: Aggregate): AttributeMap[Alias]` | Same for Aggregate |
| `getAliasMap(exprs: Iterable[NamedExpression]): AttributeMap[Alias]` | Generic version |
| `replaceAlias(expr: Expression, aliasMap: AttributeMap[Alias]): Expression` | Substitutes attributes with their aliased expressions |
| `replaceAliasWhileTracking(expr, aliasMap): (Expression, AttributeMap[Alias])` | Like `replaceAlias` but also returns which aliases were substituted |
| `replaceAliasButKeepName(expr: NamedExpression, aliasMap): NamedExpression` | Replaces alias expression but preserves original name |
| `trimAliases(e: Expression): Expression` | Removes all nested `Alias` wrappers |
| `trimNonTopLevelAliases[T <: Expression](e: T): T` | Keeps the outermost alias, strips inner ones |
| `mergeAndTrimAliases(alias: Alias): Alias` | Collapses a chain of nested aliases into one |

### 5.3 `SQLConfHelper`

Mixed into `Rule` automatically. Access config as:

```scala
conf.constraintPropagationEnabled  // Boolean
conf.optimizerMaxIterations        // Int
conf.cboEnabled                    // Boolean
// ... etc.
```

---

## 6. Writing a New Rule — Patterns and Recipes

### 6.1 Minimal rule skeleton

```scala
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._

object MyRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(FILTER), ruleId) {
      case filter: Filter if shouldRewrite(filter) =>
        rewrite(filter)
    }

  private def shouldRewrite(f: Filter): Boolean = ...
  private def rewrite(f: Filter): LogicalPlan = ...
}
```

### 6.2 Structural rewrite (eliminate / push / merge a plan node)

Pattern: match a node, inspect its child, build a new subtree.

```scala
// Example: eliminate a Filter whose condition is always true
object EliminateTrivialFilter extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(FILTER), ruleId) {
      case Filter(TrueLiteral, child) => child
    }
}
```

When constructing a replacement node, use the case-class `copy()` method to update only the changed fields:

```scala
case f @ Filter(cond, _) =>
  f.copy(condition = simplify(cond))       // keeps child unchanged

case j @ Join(_, _, _, Some(cond), _) =>
  j.copy(condition = Some(rewriteCond(cond)))
```

Use `withNewChildren` when you have a list of new children in order:

```scala
plan.withNewChildren(Seq(newLeft, newRight))
```

### 6.3 Expression-level rewrite

Pattern: leave plan structure unchanged, rewrite expressions within nodes.

```scala
object SimplifyNullComparisons extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformExpressionsWithPruning(
      _.containsAnyPattern(NULL_LITERAL, BINARY_COMPARISON), ruleId) {
      case EqualTo(left, Literal(null, _)) => IsNull(left)
      case EqualTo(Literal(null, _), right) => IsNull(right)
    }
}
```

`transformExpressions*` applies the rule to every expression slot of the plan node but does **not** recurse into child plan nodes.

### 6.4 Push-down pattern

Pattern: a parent node's property can be computed lower in the tree, reducing the data processed.

```scala
object PushFilterThroughProject extends Rule[LogicalPlan]
    with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(
      _.containsAllPatterns(FILTER, PROJECT), ruleId) {

      case filter @ Filter(cond, project @ Project(projectList, child)) =>
        val aliasMap = getAliasMap(project)
        val (pushable, nonPushable) = splitConjunctivePredicates(cond).partition { pred =>
          // After replacing aliases, can the predicate be evaluated below the project?
          canEvaluate(replaceAlias(pred, aliasMap), child)
        }
        if (pushable.isEmpty) {
          filter  // nothing to push, return unchanged
        } else {
          val pushedCond = pushable.map(replaceAlias(_, aliasMap)).reduce(And)
          val newChild = Filter(pushedCond, child)
          val newProject = project.copy(child = newChild)
          if (nonPushable.isEmpty) {
            newProject
          } else {
            Filter(nonPushable.reduce(And), newProject)
          }
        }
    }
}
```

### 6.5 Predicate splitting and reconstruction

```scala
val parts: Seq[Expression] = splitConjunctivePredicates(condition)
val (canPush, mustStay) = parts.partition(canPushDown)

val pushedCondition: Expression = canPush.reduce(And)     // naive linear tree
val balanced: Expression = buildBalancedPredicate(canPush, And)  // balanced tree (preferred for large lists)
val original: Expression = parts.reduceLeft(And)
```

### 6.6 Alias substitution

When pushing a predicate through a `Project` or `Aggregate`, attributes in the predicate may refer to aliased expressions that only exist in the project's output. Replace them before pushing:

```scala
val aliasMap: AttributeMap[Alias] = getAliasMap(project)
val rewritten: Expression = replaceAlias(predicate, aliasMap)
```

After replacement `rewritten` references the underlying expressions, which are available below the project.

### 6.7 Preventing infinite reapplication with `TreeNodeTag`

Some rules add new nodes that would match the same rule again. Guard with a tag:

```scala
private val processedTag = TreeNodeTag[Unit]("my.rule.processed")

object MyRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(JOIN), ruleId) {
      case join: Join if !join.containsTag(processedTag) =>
        val newJoin = rewrite(join)
        newJoin.setTagValue(processedTag, ())
        newJoin
    }
}
```

### 6.8 Precondition checks

| Check | When to use |
|---|---|
| `plan.resolved` | Guard analysis-time rules that should not run until analysis is complete |
| `plan.childrenResolved` | In the Analyzer (not Optimizer); wait until children resolve before resolving parent |
| `!plan.isStreaming` / `!child.isStreaming` | Many structural rules are unsafe on streaming plans |
| `child.maxRows.exists(_ <= 0)` | Skip operators over empty relations |
| `node.containsTag(someTag)` | Skip already-processed nodes |

### 6.9 Node construction: `.copy()` vs `withNewChildren()`

| Use | When |
|---|---|
| `node.copy(field = newValue)` | Changing a **non-child** field (condition, expressions, flags). Children stay the same. |
| `node.copy(child = newChild)` | Changing a **child** that is a named field (readable, safe). |
| `node.withNewChildren(Seq(...))` | You have a `Seq[LogicalPlan]` in children order; useful in generic traversal helpers. |
| `node.mapChildren(f)` | Apply the same transformation to all children uniformly. |

### 6.10 Attribute identity rules

- **Never reuse an `exprId`** from an existing attribute for a new attribute — they will be treated as the same column.
- If you need a fresh copy of an attribute (e.g. for a new output column), call `attr.newInstance()` or construct a new `AttributeReference` (it will auto-assign a new `exprId`).
- When you wrap an expression in an `Alias`, the `Alias.exprId` identifies the output column; downstream references must use `alias.toAttribute` (which carries the same `exprId`).

### 6.11 Idempotence

`Once` batches enforce idempotence in tests: running the rule twice must yield the same plan. Patterns that ensure idempotence:
- Check that the transformation condition is false after the transformation.
- Use a `TreeNodeTag` to mark processed nodes.
- Use semantic checks (e.g. `sameResult`) rather than structural checks that might trip on isomorphic subgraphs.

---

## 7. Registering the Rule in Optimizer.scala

### 7.1 Choosing the right batch

| Batch / hook | Use for |
|---|---|
| `"Finish Analysis"` `FixedPoint(1)` | Post-analysis cleanup that must run before all other optimizations. |
| `"Replace Operators"` `FixedPoint` | Logical rewrites that eliminate non-standard operators (EXCEPT, INTERSECT, DISTINCT). |
| `extendedOperatorOptimizationRules` (`FixedPoint`) | **Default choice for most new rules.** Appended to the "Operator Optimization" batch. Interacts freely with all other rules in that batch. |
| `earlyScanPushDownRules` (`Once`) | Pushing filters/projections into connector scan nodes. Stats are not yet available here. |
| `preCBORules` (`Once`) | Transformations that must complete before cost-based join reordering. |
| `"Eliminate Sorts"` `Once` | Sort elimination rules. |

### 7.2 Adding the rule

In `SparkOptimizer` (or a custom subclass):

```scala
// Option A — append to the "Operator Optimization" fixed-point batch
override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
  super.extendedOperatorOptimizationRules ++ Seq(MyNewRule)

// Option B — add a dedicated batch
override def defaultBatches: Seq[Batch] =
  super.defaultBatches :+ Batch("My Optimization", Once, MyNewRule)
```

In `Optimizer.scala` (for upstream contribution), add the rule to `operatorOptimizationRuleSet` or a dedicated batch in `defaultBatches`.

### 7.3 Non-excludable rules

If your rule is a correctness requirement (not just an optimisation), add it to `nonExcludableRules`:

```scala
override def nonExcludableRules: Seq[String] =
  super.nonExcludableRules ++ Seq(MyNewRule.ruleName)
```

---

## 8. Testing

### 8.1 Test infrastructure

Optimizer rule tests extend `PlanTest` which mixes in `PlanTestBase` and `SparkFunSuite`:

```scala
// sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/plans/PlanTest.scala
trait PlanTest extends SparkFunSuite with PlanTestBase
```

Key assertion helpers from `PlanTestBase`:

```scala
comparePlans(actual: LogicalPlan, expected: LogicalPlan, checkAnalysis: Boolean = true): Unit
// Normalises exprIds in both plans before comparing. Prints a side-by-side diff on failure.

compareExpressions(e1: Expression, e2: Expression): Unit
// Wraps each in a Filter and calls comparePlans.

normalizeExprIds(plan: LogicalPlan): LogicalPlan
normalizePlan(plan: LogicalPlan): LogicalPlan
```

### 8.2 Test suite skeleton

```scala
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MyRuleSuite extends PlanTest {

  // Build a minimal RuleExecutor that applies only the rules under test
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MyRule", FixedPoint(10), MyRule) :: Nil
  }

  // Cheap test relation — no actual data, just schema
  val testRelation = LocalRelation($"a".int, $"b".int, $"c".string)

  test("rule eliminates X") {
    // Build a plan using the DSL
    val input = testRelation
      .where($"a" > 0 && Literal(true))  // trivially simplifiable
      .select($"a", $"b")
      .analyze                             // resolve all attributes

    val optimized = Optimize.execute(input)

    val expected = testRelation
      .where($"a" > 0)
      .select($"a", $"b")
      .analyze

    comparePlans(optimized, expected)
  }
}
```

### 8.3 DSL quick reference

```scala
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

// Attributes
$"colName".int          // AttributeReference("colName", IntegerType)
$"colName".string
$"colName".long
$"colName".boolean
$"colName".int.notNull  // nullable = false

// Plan builders (return LogicalPlan)
relation.where(condition)
relation.select(exprs: _*)
relation.groupBy(keys: _*)(aggs: _*)
relation.orderBy(orders: _*)
relation.limit(n)
relation.join(right)
relation.analyze      // triggers analysis; required before comparePlans
```

### 8.4 Test file location

```
sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/optimizer/
```

Name the file `<RuleName>Suite.scala`. See `BooleanSimplificationSuite.scala` or `ReplaceNullWithFalseInPredicate` tests as representative examples.

---

## Quick Reference: Source Files

| Topic | File |
|---|---|
| `Rule[T]` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/Rule.scala` |
| `RuleExecutor[T]` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/RuleExecutor.scala` |
| `Optimizer` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala` |
| `LogicalPlan` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlan.scala` |
| `QueryPlan` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/QueryPlan.scala` |
| `TreeNode` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala` |
| `TreePattern` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreePatterns.scala` |
| `Expression` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Expression.scala` |
| `NamedExpression`, `AttributeReference`, `Alias`, `ExprId` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala` |
| `AttributeSet` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/AttributeSet.scala` |
| `PredicateHelper` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/predicates.scala` |
| `AliasHelper` | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/AliasHelper.scala` |
| Logical operators | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala` |
| Representative rules | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/ReplaceNullWithFalseInPredicate.scala` |
| | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PropagateEmptyRelation.scala` |
| | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala` |
| | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala` |
| | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/RemoveRedundantSorts.scala` |
| | `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/PushDownLeftSemiAntiJoin.scala` |
| Test base | `sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/plans/PlanTest.scala` |

---

## 9. Expression vs Attribute

### What they are

**`Expression`** is the general base class for anything that can be evaluated on a row to produce a value. It is a tree node and can contain children (sub-expressions).

Examples: `a + 1`, `CASE WHEN a > 0 THEN 1 ELSE 0 END`, `length(name)`, `Literal(42)`, `IsNull(col)`

**`Attribute`** is a *leaf* expression that represents a **named column coming from a plan's output**. It has no children — it just says "read column X from the incoming row." `AttributeReference` is the only production subclass.

```scala
// Attribute IS-A Expression (leaf, reads a column by exprId)
abstract class Attribute extends LeafExpression with NamedExpression

// AttributeReference IS-A Attribute
case class AttributeReference(name: String, dataType: DataType, nullable: Boolean)(
    val exprId: ExprId, ...)
```

### The key distinction

An `Attribute` carries an **`exprId`** — a globally unique identifier that says *which logical column* this is, independent of its name or position. Two `AttributeReference`s with the same `exprId` are the same column even if they have different names (e.g. after an alias rename).

An `Expression` in general has no such identity — it is a computation, not a column reference.

```
plan.output: Seq[Attribute]    // "what columns does this plan produce"
expr.references: AttributeSet  // "which columns does this expression consume"
```

`plan.output` is always `Seq[Attribute]` — never a general `Seq[Expression]` — because a plan's output must consist of nameable columns with stable identity.

### `Alias` — bridging the two

When you want to give a general expression a name and make it appear in `output`, you wrap it in an `Alias`:

```scala
Alias(a + 1, "a_plus_one")(exprId = NamedExpression.newExprId)
```

`Alias` IS-A `NamedExpression` and IS-A `Expression`. Calling `.toAttribute` on it returns an `AttributeReference` with the same `exprId`, which is what ends up in `Project.output`.

```
Project(List(Alias(a + 1, "x")(id=5)), child)
         ^                                ^
         expression (computation)         output attribute: AttributeRef("x", id=5)
```

Downstream plan nodes reference column `x` as `AttributeReference("x", id=5)`, not as `a + 1`.

### When to use which

| Situation | Use |
|---|---|
| Reading from a plan's output (e.g. a filter condition) | `AttributeReference` — references a column by `exprId` |
| Computing something new (e.g. `a + b`, `coalesce(x, 0)`) | `Expression` subclass |
| Naming an expression so it appears in `output` | Wrap in `Alias` — gives the expression an `exprId` and a name |
| Checking if two column references are the same column | Compare `exprId`s, not names: `attr1.exprId == attr2.exprId` |
| Checking if a plan can compute an expression | `canEvaluate(expr, plan)` — checks `expr.references ⊆ plan.outputSet` |
| `output` / `outputSet` on a plan | Always `Seq[Attribute]` / `AttributeSet` |
| `projectList` in `Project`, `aggregateExpressions` in `Aggregate` | `Seq[NamedExpression]` — either bare `Attribute`s (pass-through) or `Alias(expr, name)` (new computation) |

### Concrete example

```scala
// relation produces: a (id=1), b (id=2)
val rel = LocalRelation($"a".int, $"b".int)

// Project: pass through 'a', compute 'a+b' as 'c'
Project(
  Seq(
    $"a",                      // AttributeReference("a", id=1) — pass column through
    Alias($"a" + $"b", "c")()  // compute a+b, expose as new column "c" (id=3)
  ),
  rel
)
// output = [AttributeRef("a", id=1), AttributeRef("c", id=3)]
```

A downstream `Filter($"c" > 0, ...)` refers to column `c` via `id=3`. It does not know or care that `c` was computed as `a + b` — that detail lives in the `Project`.

---

## 10. Converting an Expression to an Attribute

Calling `.toAttribute` (or wrapping in `Alias`) is a **schema declaration**, not evaluation. No computation happens.

### What `.toAttribute` actually does

```scala
// Inside Alias
override def toAttribute: Attribute =
  AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifier)
```

It creates a lightweight `AttributeReference` that borrows the `exprId` from the alias. The child expression (`a + b`) is not touched, not evaluated, not even inspected.

### What the `exprId` linkage means

Think of it as a **promise**, not a result:

```
Alias(a + b, "c")(exprId = 42)   ← "I will compute a+b and call the result column #42"
                                        |
AttributeReference("c", id=42)    ← "give me whatever column #42 is"
```

The `Alias` stays in `Project.projectList` — that is where the computation lives. The `AttributeReference` goes into `Project.output` and into any downstream expression that references `c`. They are linked by `exprId = 42`, but the computation only happens at **execution time** when the physical plan evaluates the `Alias` node on a real row.

### The full lifecycle

```
Plan construction   Alias(a+b, "c")        ← expression defined, not evaluated
                         |
                         | .toAttribute
                         v
Analysis            AttributeRef("c", id=42) added to Project.output
                         |
Optimization        Rules rewrite the plan tree — still no evaluation
                         |
Physical planning   Project → physical ProjectExec
                         |
Execution           For each row: eval Alias(a+b) → produces an integer
                                         ^
                                         evaluation happens here
```

### Summary

`.toAttribute` gives the expression a **stable name and identity** in the schema so that downstream operators can refer to it by `exprId`. It is purely a metadata operation — equivalent to writing a variable declaration. The expression is only evaluated when the physical plan runs on actual data.
