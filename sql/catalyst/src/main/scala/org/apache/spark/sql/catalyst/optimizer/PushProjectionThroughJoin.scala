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

import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern

object PushProjectionThroughJoin extends Rule[LogicalPlan] {
  // FIXME: Write notes
  // transformUpWithPruning is post over traversal
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAnyPattern(TreePattern.INNER_LIKE_JOIN, TreePattern.PROJECT)) {
    case p @ Project(projectList, join @ Join(leftChild, rightChild, Cross | Inner, condition, _))
        if eligibleForOptimization(projectList) =>
      val expressionsFromLeftSize = filterExpressions(projectList, leftChild)
      val expressionsFromRightSize = filterExpressions(projectList, rightChild)
      val expressionFromBothSides = projectList
        .diff(expressionsFromLeftSize)
        .diff(expressionsFromRightSize)

      val leftAttrForJoinCondition = filterJoinAttributes(condition, leftChild)
      val rightAttrForJoinCondition = filterJoinAttributes(condition, rightChild)

      if (expressionFromBothSides.size == projectList.size) {
        p
      } else {
        val newLeft = if (eligibleForOptimization(expressionsFromLeftSize)) {
          Project(expressionsFromLeftSize ++ leftAttrForJoinCondition, leftChild)
        } else {
          leftChild
        }
        val newRight = if (eligibleForOptimization(expressionsFromRightSize)) {
          Project(expressionsFromRightSize ++ rightAttrForJoinCondition, rightChild)
        } else {
          rightChild
        }
        val newJoin = join.copy(left = newLeft, right = newRight)
        val updatedProjectList = updatedProjectListReference(projectList, newJoin)
        Project(updatedProjectList, newJoin)
      }
  }

  private def updatedProjectListReference(
      projectList: Seq[NamedExpression],
      newJoin: LogicalPlan): Seq[NamedExpression] = {
    val attrMap = AttributeMap(newJoin.output.map(a => a.toAttribute -> a))
    val updateProjectList = projectList.map {
      case a: NamedExpression if attrMap.contains(a.toAttribute) => attrMap(a.toAttribute)
      case e => e
    }
    updateProjectList.distinct
  }

  private def filterExpressions(
      projectList: Seq[NamedExpression],
      child: LogicalPlan): Seq[NamedExpression] = {
    // TODO: Test for binary expression
    projectList.filter(expr => expr.references.subsetOf(child.outputSet))
  }

  private def filterJoinAttributes(joinCondition: Option[Expression], child: LogicalPlan) = {
    joinCondition
      .map {
        _.references.toSeq.filter(attr => child.outputSet.contains(attr)).toSeq
      }
      .getOrElse(Seq.empty)
  }

  private def eligibleForOptimization(projectList: Seq[NamedExpression]): Boolean = {
    // We can only push down projections that are deterministic and do not contain simple attributes
    // 1. Pushing down simple attributes is doesnt not makes sense because there is not point of
    // early evaluation. Adding Project will add to computational overhead and cost us time.
    // 2. Pushing down deterministic expressions is not possible because we cannot guarantee that ??
    // Should do partial expression pushdown i.e push down only deterministic expressions ?
    !projectList.forall(_.isInstanceOf[AttributeReference]) && projectList.forall(_.deterministic)
  }
}
