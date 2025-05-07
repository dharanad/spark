/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PushProjectionThroughJoinSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Batch] =
      Batch("PushProjectionThroughJoin", FixedPoint(10), PushProjectionThroughJoin) :: Nil
  }

  val usersRelation: LocalRelation = LocalRelation.fromExternalRows(
    Seq("uid".attr.int, "name".attr.string),
    Seq(Row(1, "Alice"), Row(2, "Bob"), Row(3, "Charlie")))

  val balanceRelation: LocalRelation = LocalRelation.fromExternalRows(
    Seq("b_uid".attr.int, "balance".attr.double),
    Seq(Row(1, 100.0), Row(2, 200.0), Row(3, 300.0)))

  test("Simple Case -- No Push down") {
    val testRelationOne = LocalRelation.fromExternalRows(
      Seq("a_uid".attr.int, "name".attr.string, "balance".attr.double),
      Seq(Row(1, "Alice", 100.0), Row(2, "Bob", 200.0), Row(3, "Charlie", 300.0)))

    val testRelationTwo = LocalRelation.fromExternalRows(
      Seq("t_uid".attr.int, "type".attr.string),
      Seq(Row(1, "savings"), Row(2, "priority"), Row(3, "wealth")))

    val query = testRelationOne
      .join(testRelationTwo, condition = Some("a_uid".attr === "t_uid".attr))
      .select($"a_uid".attr, $"name".attr, $"balance".attr, $"type".attr)
      .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer = query.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("Simple Case -- Push down left side") {

    val query = usersRelation
      .join(balanceRelation, condition = Some("uid".attr === "b_uid".attr))
      .select(upper($"name").as("name"), $"balance".attr)
      .analyze

    val optimized = Optimize.execute(query)

    val expected = usersRelation
      .select(upper($"name").as("name"), $"uid".attr)
      .join(balanceRelation, condition = Some("uid".attr === "b_uid".attr))
      .select($"name".attr, $"balance".attr)
      .analyze

    comparePlans(optimized, expected)
  }

  test("Simple Case -- Push down right side") {

    val query = usersRelation
      .join(balanceRelation, condition = Some("uid".attr === "b_uid".attr))
      .select($"name".attr, abs($"balance".attr).as("balance"))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = usersRelation
      .join(
        balanceRelation.select(abs($"balance".attr).as("balance"), $"b_uid".attr),
        condition = Some("uid".attr === "b_uid".attr))
      .select($"name".attr, $"balance".attr)
      .analyze

    comparePlans(optimized, expected)
  }

  test("Simple Case -- Push down both sides") {
    val query = usersRelation
      .join(balanceRelation, condition = Some("uid".attr === "b_uid".attr))
      .select(upper($"name").as("name"), abs($"balance".attr).as("balance"))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = usersRelation
      .select(upper($"name").as("name"), $"uid".attr)
      .join(
        balanceRelation.select(abs($"balance".attr).as("balance"), $"b_uid".attr),
        condition = Some("uid".attr === "b_uid".attr))
      .select($"name".attr, $"balance".attr)
      .analyze

    comparePlans(optimized, expected)
  }

  test("Simple Case -- Push down partial expression") {
    val query = usersRelation
      .join(balanceRelation, condition = Some("uid".attr === "b_uid".attr))
      .select(
        intToLiteral(10).as("a"),
        upper($"name").as("name"),
        abs($"balance".attr).as("balance"))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = usersRelation
      .select($"uid".attr, upper($"name").as("name"))
      .join(
        balanceRelation.select($"b_uid".attr, abs($"balance".attr).as("balance")),
        condition = Some("uid".attr === "b_uid".attr))
      .select(intToLiteral(10).as("a"), $"name".attr, $"balance".attr)
      .analyze

    comparePlans(optimized, expected)
  }

  // TODO: Add tests with non-deterministic expressions
  // TODO: Add tests case where join is highly selective, where pushing down is costly
}
