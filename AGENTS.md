# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Apache Spark

## Pre-flight Checks

Before the first code edit or running test in a session, ensure a clean working environment. DO NOT skip these checks:

1. Run `git remote -v` to identify the personal fork and upstream (`apache/spark`). If unclear, ask the user to configure their remotes following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).
2. If the latest commit on `<upstream>/master` is more than a day old (check with `git log -1 --format="%ci" <upstream>/master`), run `git fetch <upstream> master`.
3. If there are uncommitted changes (check with `git status`), ask the user to stash them before proceeding.
4. Switch to the appropriate branch:
   - **Existing PR**: resolve the PR branch name via `gh api repos/apache/spark/pulls/<number> --jq '.head.ref'`, then look for a local branch matching that name. If found, switch to it and inform the user. If not found, ask whether to fetch it or if there is a local branch under a different name.
   - **New edits**: ask the user to choose: create a new git worktree from `<upstream>/master` and work from there (recommended), or create and switch to a new branch from `<upstream>/master`.
   - **Running tests**: use `<upstream>/master`.

## Development Notes

SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class documentation before running or updating these tests. DO NOT edit the generated golden files (`.sql.out`) directly. Always regenerate them when needed, and carefully review the diff to make sure it's expected.

Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`. Read the README there before modifying proto definitions.

Avoid introducing non-ASCII characters in code or comments. String literals may contain non-ASCII when the content requires it (error messages, test data, etc.). Identifiers are ASCII by convention. The common failure mode is typographic characters (em-dash, smart quotes, ellipsis, non-breaking space) sneaking into comments; scalastyle flags some of these. Spot-check before committing: `grep -rn -P "[^\x00-\x7F]" <files>`.

## Build and Test

Build and tests can take a long time. Before running tests, ask the user if they have more changes to make.

Prefer SBT over Maven for faster incremental compilation. Module names are defined in `project/SparkBuild.scala`. Common ones:

| SBT module ID | Source directory |
|---|---|
| `core` | `core/` |
| `catalyst` | `sql/catalyst/` |
| `sql` | `sql/core/` |
| `sql-api` | `sql/api/` |
| `hive` | `sql/hive/` |
| `hive-thriftserver` | `sql/hive-thriftserver/` |
| `connect-common` | `sql/connect/common/` |
| `connect` | `sql/connect/server/` |
| `connect-client-jvm` | `sql/connect/client/jvm/` |
| `streaming` | `streaming/` |
| `mllib` | `mllib/` |
| `graphx` | `graphx/` |
| `launcher` | `launcher/` |
| `network-common` | `common/network-common/` |
| `common-utils` | `common/utils/` |
| `unsafe` | `common/unsafe/` |
| `variant` | `common/variant/` |
| `pipelines` | `sql/pipelines/` |

Compile a single module:

    build/sbt <module>/compile

Compile test code for a single module:

    build/sbt <module>/Test/compile

Run test suites by wildcard or full class name:

    build/sbt '<module>/testOnly *MySuite'
    build/sbt '<module>/testOnly org.apache.spark.sql.MySuite'

Run test cases matching a substring:

    build/sbt '<module>/testOnly *MySuite -- -z "test name"'

For faster iteration, keep SBT open in interactive mode:

    build/sbt
    > project <module>
    > testOnly *MySuite

### Linting and Style

Run Scala style checks (scalastyle + scalafmt for Connect modules):

    dev/lint-scala

Run Python linting (flake8, ruff, mypy):

    dev/lint-python

Run Java checkstyle:

    dev/lint-java

Scalafmt is enforced only on `sql/api`, `sql/connect/**`. To auto-format those modules:

    ./build/mvn scalafmt:format -Dscalafmt.skip=false -Dscalafmt.validateOnly=false \
      -Dscalafmt.changedOnly=false \
      -pl sql/api -pl sql/connect/common -pl sql/connect/server \
      -pl sql/connect/shims -pl sql/connect/client/jvm

### PySpark Tests

PySpark tests require building Spark with Hive support first:

    build/sbt -Phive package

Activate the virtual environment specified by the user, or default to `.venv`:

    source <venv>/bin/activate

If the default venv does not exist, create it:

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r dev/requirements.txt

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Investigating PR CI Failures

Do NOT download full job logs to grep for errors — they are very large and slow. Instead, use the test report annotations on the fork.

Step 1 — Get the fork owner and the latest commit SHA of the PR:

    gh api repos/apache/spark/pulls/<PR_NUMBER> --jq '{owner: .head.repo.owner.login, sha: .head.sha}'

Step 2 — Find the "Report test results" check run on the fork's commit:

    gh api repos/<OWNER>/spark/commits/<SHA>/check-runs \
      --jq '.check_runs[] | select(.name == "Report test results") | {id: .id, annotations: .output.annotations_count}'

Step 3 — Fetch failure annotations:

    gh api repos/<OWNER>/spark/check-runs/<CHECK_RUN_ID>/annotations

Each annotation contains the test class, test name, and failure message.

## Code Architecture

### SQL Query Execution Pipeline

A SQL query or DataFrame operation passes through these stages (all lazy, in `QueryExecution`):

```
SQL text / DataFrame API
        |
        v
  Unresolved LogicalPlan  (parser: sql/catalyst — ANTLR grammar in sql/catalyst/src/main/antlr4/)
        |
        v  Analyzer (sql/catalyst/src/main/scala/.../analysis/Analyzer.scala)
           — resolves attribute references, functions, types using the Catalog
        |
        v  Analyzed LogicalPlan
        |
        v  Optimizer (sql/catalyst/src/main/scala/.../optimizer/Optimizer.scala)
           — rule-based rewrites in batches (see optimizer/ directory for all rules)
        |
        v  Optimized LogicalPlan
        |
        v  SparkPlanner (sql/core — SparkStrategies.scala)
           — converts logical operators to physical operators (SparkPlan)
        |
        v  SparkPlan (unexecuted)
        |
        v  preparations: Seq[Rule[SparkPlan]]
           — EnsureRequirements (adds exchanges/sorts), CollapseCodegenStages,
             InsertAdaptiveSparkPlan, etc.
        |
        v  Executed SparkPlan → RDD[InternalRow] → results
```

The `Analyzer`, `Optimizer`, and all rule batches use `RuleExecutor[LogicalPlan]` — see `docs/catalyst-optimizer-rule-guide.md` for a detailed developer guide on writing new rules.

### SQL Subprojects

| Module | Role |
|---|---|
| `sql/api` | Public types shared between Catalyst and Connect client (DataType, Row, etc.) |
| `sql/catalyst` | Implementation-agnostic logical plan tree, expressions, analysis, and optimization framework |
| `sql/core` | Query planner, physical execution (SparkPlan), DataFrameReader/Writer, SparkSession |
| `sql/hive` | HiveQL support, Hive Metastore integration, Hive SerDe |
| `sql/connect` | Spark Connect — gRPC-based remote execution protocol (proto + server + client) |
| `sql/pipelines` | Declarative pipelines (DLT-style) |

### Physical Execution — SparkStrategies

`SparkStrategies.scala` (sql/core) defines `Strategy` objects that pattern-match on `LogicalPlan` nodes and emit `Seq[SparkPlan]`. Key strategies: `JoinSelection`, `Aggregation`, `BasicOperators`, `Window`, `InMemoryScans`. The planner calls all strategies and takes the first non-empty result.

### Adaptive Query Execution (AQE)

When AQE is enabled (`spark.sql.adaptive.enabled=true`), `InsertAdaptiveSparkPlan` wraps the physical plan. At runtime, shuffle statistics are used to re-optimize join strategies and partition counts. The AQE code lives in `sql/core/src/main/scala/.../execution/adaptive/`.

## Pull Request Workflow

PR title format is `[SPARK-xxxx][COMPONENT] Title`. The component tag is derived from the JIRA component name: take the last word and uppercase it (e.g. `Project Infra` → `[INFRA]`, `Spark Core` → `[CORE]`, `Structured Streaming` → `[STREAMING]`, `SQL` → `[SQL]`).

Infer the PR title from the changes. If no ticket ID is given, create one using `dev/create_spark_jira.py`, using the PR title (without the JIRA ID and component tag) as the ticket title.

    python3 dev/create_spark_jira.py "<title>" -c <component> { -t <type> | -p <parent-jira-id> }

- **Component** (`-c`): the exact JIRA component name (not the PR title shorthand), e.g. "SQL", "Spark Core", "PySpark", "Connect". Run `python3 dev/create_spark_jira.py --list-components` for the full list.
- **Issue type** (`-t`): "Bug", "Improvement", "New Feature", "Test", "Documentation", or "Dependency upgrade".
- **Parent** (`-p`): if the user mentions a parent JIRA ticket (e.g., "this is a subtask of SPARK-12345"), pass it instead of `-t`. The issue type is automatically "Sub-task".

The script sets the latest unreleased version as the default affected version.

After creating a JIRA ticket, print a prominent notice so the user does not miss it:

    ============================================================
    JIRA ticket created: SPARK-XXXXX
    https://issues.apache.org/jira/browse/SPARK-XXXXX

    Title:              <title>
    Component(s):       <component>
    Issue type:         <type>
    Affected version(s): <version>
    Priority:           <priority>

    Please review and adjust these fields if needed.
    ============================================================

Before writing the PR description, read `.github/PULL_REQUEST_TEMPLATE` and fill in every section from that file.

DO NOT push to the upstream repo. Always push to the personal fork. Open PRs against `master` on the upstream repo.

DO NOT force push or use `--amend` on pushed commits unless the user explicitly asks. If the remote branch has new commits, fetch and rebase before pushing.

Always get user approval before external operations such as pushing commits, creating PRs, or posting comments. Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub PR URL for the user and recommend installing the GitHub CLI.
